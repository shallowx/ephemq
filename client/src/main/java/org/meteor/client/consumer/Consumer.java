package org.meteor.client.consumer;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.UnsafeByteOperations;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.*;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.meteor.client.internal.*;
import org.meteor.client.util.TopicPatterns;
import org.meteor.remote.proto.server.*;
import org.meteor.common.message.Extras;
import org.meteor.common.message.MessageId;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.common.thread.FastEventExecutor;
import org.meteor.remote.proto.MessageMetadata;
import org.meteor.remote.proto.client.MessagePushSignal;
import org.meteor.remote.proto.client.NodeOfflineSignal;
import org.meteor.remote.proto.client.TopicChangedSignal;
import org.meteor.remote.util.NetworkUtils;
import org.meteor.remote.util.ProtoBufUtils;

import javax.annotation.Nonnull;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class Consumer implements MeterBinder {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(Consumer.class);
    private static final String METRICS_NETTY_PENDING_TASK_NAME = "consumer_netty_pending_task";
    private final String name;
    private final ConsumerConfig consumerConfig;
    private final MessageListener listener;
    private final Client client;
    private final Map<String, Map<String, Mode>> wholeQueueTopics = new ConcurrentHashMap<>();
    private final Map<Integer, ClientChannel> ledgerChannels = new ConcurrentHashMap<>();
    private final Map<Integer, AtomicReference<MessageId>> ledgerSequences = new ConcurrentHashMap<>();
    private final AtomicBoolean changeTaskTouched = new AtomicBoolean(false);
    private final Map<String, Long> topicTokens = new HashMap<>();
    private final Map<String, Map<Integer, Integer>> topicLedgerVersions = new HashMap<>();
    private final Map<String, Map<Integer, Int2IntMap>> topicLedgerMarkers = new HashMap<>();
    private final Map<String, Future<?>> obsoleteFutures = new ConcurrentHashMap<>();
    private EventExecutor executor;
    private EventExecutorGroup group;
    private MessageHandler[] handlers;
    private volatile Boolean state;
    private Future<?> failedRetryFuture;
    private Map<String, Map<String, Mode>> failedRecords = new HashMap<>();

    public Consumer(String name, ConsumerConfig consumerConfig, MessageListener listener) {
        this.name = name;
        this.consumerConfig = Objects.requireNonNull(consumerConfig, "Consumer config not found");
        this.listener = Objects.requireNonNull(listener, "Consumer message listener not found");
        this.client = new Client(name, consumerConfig.getClientConfig(), new ConsumerListener());
    }

    public synchronized void start() {
        if (state != null) {
            logger.warn("The client[{}] war started", name);
            return;
        }

        state = Boolean.TRUE;
        client.start();
        executor = new FastEventExecutor(new DefaultThreadFactory("consumer-task"));
        int shardCount = Math.max(consumerConfig.getHandlerThreadCount(), consumerConfig.getHandlerShardCount());
        handlers = new MessageHandler[shardCount];
        group = NetworkUtils.newEventExecutorGroup(consumerConfig.getHandlerThreadCount(), "consumer-message-group");
        int handlerPendingCount = consumerConfig.getHandlerPendingCount();
        for (int i = 0; i < shardCount; i++) {
            Semaphore semaphore = new Semaphore(handlerPendingCount);
            MessageHandler messageHandler = new MessageHandler(String.valueOf(i), semaphore, group.next());
            this.handlers[i] = messageHandler;
        }

        executor.scheduleWithFixedDelay(this::touchChangedTask, 30, 30, TimeUnit.SECONDS);
    }

    @Override
    public void bindTo(@Nonnull MeterRegistry meterRegistry) {
        {
            SingleThreadEventExecutor singleThreadEventExecutor = (SingleThreadEventExecutor) executor;
            Gauge.builder(METRICS_NETTY_PENDING_TASK_NAME, singleThreadEventExecutor, SingleThreadEventExecutor::pendingTasks)
                    .tag("type", "consumer-task")
                    .tag("name", name)
                    .tag("id", singleThreadEventExecutor.threadProperties().name())
                    .register(meterRegistry);
        }

        for (EventExecutor eventExecutor : group) {
            try {
                SingleThreadEventExecutor executor = (SingleThreadEventExecutor) eventExecutor;
                Gauge.builder(METRICS_NETTY_PENDING_TASK_NAME, executor, SingleThreadEventExecutor::pendingTasks)
                        .tag("type", "consumer-handler")
                        .tag("name", name)
                        .tag("id", executor.threadProperties().name())
                        .register(meterRegistry);
            } catch (Throwable ignored) {
            }
        }
    }

    public boolean subscribe(String topic, String queue) {
        TopicPatterns.validateTopic(topic);
        TopicPatterns.validateQueue(queue);

        topic = topic.intern();
        synchronized (wholeQueueTopics) {
            Map<String, Mode> topicModes = wholeQueueTopics.computeIfAbsent(queue, k -> new ConcurrentHashMap<>());
            Mode mode = topicModes.get(topic);
            if (mode == null) {
                topicModes.put(topic, Mode.APPEND);
            } else if (mode == Mode.DELETE) {
                topicModes.put(topic, Mode.REMAIN);
            } else {
                return false;
            }
        }

        touchChangedTask();
        return true;
    }

    public boolean deSubscribe(String topic, String queue) {
        TopicPatterns.validateTopic(topic);
        TopicPatterns.validateQueue(queue);

        topic = topic.intern();
        synchronized (wholeQueueTopics) {
            Map<String, Mode> topicModes = wholeQueueTopics.get(queue);
            if (topicModes == null) {
                return false;
            }

            Mode mode = topicModes.get(topic);
            if (mode == Mode.REMAIN) {
                topicModes.put(topic, Mode.DELETE);
            } else if (mode == Mode.APPEND) {
                topicModes.remove(topic);
            } else {
                return false;
            }
            if (topicModes.isEmpty()) {
                wholeQueueTopics.remove(queue);
            }
        }
        touchChangedTask();
        return true;
    }

    public boolean clear(String topic) {
        TopicPatterns.validateTopic(topic);

        topic = topic.intern();
        boolean cleared = false;
        synchronized (wholeQueueTopics) {
            Iterator<Map.Entry<String, Map<String, Mode>>> iterator = wholeQueueTopics.entrySet().iterator();
            while (iterator.hasNext()) {
                Map<String, Mode> topicModes = iterator.next().getValue();
                Mode mode = topicModes.get(topic);
                if (mode == Mode.REMAIN) {
                    topicModes.put(topic, Mode.DELETE);
                } else if (mode == Mode.APPEND) {
                    topicModes.remove(topic);
                    cleared = true;
                } else {
                    continue;
                }

                if (topicModes.isEmpty()) {
                    iterator.remove();
                }
            }
        }

        if (cleared) {
            touchChangedTask();
        }
        return cleared;
    }

    private void touchChangedTask() {
        if (executor.isShuttingDown() || !changeTaskTouched.compareAndSet(false, true)) {
            return;
        }

        try {
            executor.execute(this::doChangeTask);
        } catch (Throwable t) {
            changeTaskTouched.set(false);
            logger.error("Consumer<{}> touch changed task execute failed, and trg again later", name);
        }
    }

    private void doChangeTask() {
        changeTaskTouched.compareAndSet(true, false);
        Map<String, Map<String, Mode>> changedRecords = clearFailedRecords();
        Map<String, Integer> topicQueueCount = extractChangedRecords(changedRecords);
        if (changedRecords.isEmpty()) {
            return;
        }

        for (Map.Entry<String, Map<String, Mode>> entry : changedRecords.entrySet()) {
            String topic = entry.getKey();
            Integer queueCount = topicQueueCount.get(topic);
            if (queueCount == null || queueCount == 0) {
                cleanTopicSubscribe(topic);
                continue;
            }

            MessageRouter router = fetchRouter(topic);
            if (router == null) {
                resetFailedRecords(topic, entry.getValue());
                continue;
            }

            Long token = topicTokens.get(topic);
            if (token != null && token > router.token()) {
                resetFailedRecords(topic, entry.getValue());
                continue;
            }

            topicTokens.put(topic, router.token());
            if (token != null && token < router.token()) {
                Map<String, Mode> failedQueues = resetTopicSubscribe(router, topic);
                resetFailedRecords(topic, failedQueues);
                continue;
            }

            Map<String, Mode> failedQueues = changeTopicSubscribe(router, topic, entry.getValue());
            resetFailedRecords(topic, failedQueues);
        }

        if (existFailedRecords()) {
            scheduleFailedRetryTask(consumerConfig.getControlRetryDelayMs());
        }
    }

    private void cleanTopicSubscribe(String topic) {
        Map<Integer, Int2IntMap> ledgerMarkers = topicLedgerMarkers.get(topic);
        if (ledgerMarkers != null) {
            for (int ledgerId : ledgerMarkers.keySet()) {
                ClientChannel channel = ledgerChannels.get(ledgerId);
                if (channel != null && channel.isActive()) {
                    doCleanSubscribe(channel, topic, ledgerId);
                }

                ledgerChannels.remove(ledgerId);
                ledgerSequences.remove(ledgerId);
            }
        }

        topicLedgerMarkers.remove(topic);
        topicLedgerVersions.remove(topic);
        topicTokens.remove(topic);
    }

    private Map<String, Mode> resetTopicSubscribe(MessageRouter router, String topic) {
        Set<String> topicQueues = dumpPresentTopic(topic);
        if (topicQueues.isEmpty()) {
            cleanTopicSubscribe(topic);
            return null;
        }

        Map<String, Mode> failedQueues = new HashMap<>();
        Map<Integer, Integer> newLedgerVersions = new HashMap<>();
        topicLedgerVersions.put(topic, newLedgerVersions);

        Map<Integer, Int2IntMap> newLedgerMarkers = new HashMap<>();
        Map<Integer, Int2IntMap> oldLedgerMarkers = topicLedgerMarkers.put(topic, newLedgerMarkers);
        oldLedgerMarkers = oldLedgerMarkers == null ? new HashMap<>() : oldLedgerMarkers;
        Map<MessageLedger, Set<String>> ledgerQueues = new HashMap<>();
        for (String queue : topicQueues) {
            MessageLedger ledger = calculateLedger(router, queue);
            if (ledger == null) {
                failedQueues.put(queue, Mode.APPEND);
                continue;
            }

            ledgerQueues.computeIfAbsent(ledger, k -> new HashSet<>()).add(queue);
        }

        for (Map.Entry<MessageLedger, Set<String>> entry : ledgerQueues.entrySet()) {
            MessageLedger ledger = entry.getKey();
            Set<String> queues = entry.getValue();

            int ledgerId = ledger.id();
            newLedgerMarkers.put(ledgerId, new Int2IntOpenHashMap(0));

            ClientChannel channel = ledgerChannels.get(ledgerId);
            if (channel != null && channel.isActive()) {
                doCleanSubscribe(channel, topic, ledgerId);
            }

            ledgerChannels.remove(ledgerId);
            channel = newLedgerChannel(topic, ledger, channel);
            if (channel == null || !channel.isActive()) {
                queues.forEach(q -> failedQueues.put(q, Mode.APPEND));
                continue;
            }

            Int2IntMap markersCounts = new Int2IntOpenHashMap(queues.size());
            for (String queue : queues) {
                int marker = router.calculateMarker(queue);
                markersCounts.mergeInt(marker, 1, Integer::sum);
            }

            ByteString markers = generateMarkers(markersCounts.keySet());
            if (markers == null) {
                queues.forEach(q -> failedQueues.put(q, Mode.APPEND));
                continue;
            }

            int epoch = -1;
            long index = -1;
            AtomicReference<MessageId> sequence = ledgerSequences.get(ledgerId);
            if (sequence != null) {
                MessageId lastId = sequence.get();
                if (lastId != null) {
                    epoch = lastId.epoch();
                    index = lastId.index();
                }
            } else {
                ledgerSequences.put(ledgerId, new AtomicReference<>());
            }

            ledgerChannels.put(ledgerId, channel);
            if (!doResetSubscribe(channel, topic, ledger.id(), epoch, index, markers)) {
                doCleanSubscribe(channel, topic, ledger.id());
                ledgerChannels.remove(ledgerId);
                queues.forEach(q -> failedQueues.put(q, Mode.APPEND));
                continue;
            }

            newLedgerVersions.put(ledgerId, ledger.version());
            newLedgerMarkers.put(ledgerId, markersCounts);
        }

        for (int ledgerId : oldLedgerMarkers.keySet()) {
            if (!newLedgerMarkers.containsKey(ledgerId)) {
                ClientChannel channel = ledgerChannels.get(ledgerId);
                if (channel != null && channel.isActive()) {
                    doCleanSubscribe(channel, topic, ledgerId);
                }

                ledgerChannels.remove(ledgerId);
                ledgerSequences.remove(ledgerId);
            }
        }
        return failedQueues.isEmpty() ? null : failedQueues;
    }

    private Map<String, Mode> changeTopicSubscribe(MessageRouter router, String topic, Map<String, Mode> changedQueues) {
        Map<String, Mode> failedQueues = new HashMap<>();
        Map<MessageLedger, Map<String, Mode>> ledgerRecords = new HashMap<>();
        for (Map.Entry<String, Mode> entry : changedQueues.entrySet()) {
            String queue = entry.getKey();
            MessageLedger ledger = calculateLedger(router, queue);
            if (ledger == null) {
                failedQueues.put(queue, entry.getValue());
                continue;
            }
            ledgerRecords.computeIfAbsent(ledger, k -> new HashMap<>()).put(queue, entry.getValue());
        }

        boolean hasFailed = false;
        for (MessageLedger ledger : router.ledgers().values()) {
            Map<String, Mode> ledgerChangedQueues = ledgerRecords.computeIfAbsent(ledger, k -> new HashMap<>());
            Map<String, Mode> ledgerFailedQueues = changeLedgerSubscribe(router, ledger, topic, ledgerChangedQueues);
            if (ledgerFailedQueues != null) {
                failedQueues.putAll(ledgerFailedQueues);
                hasFailed = true;
            }
        }

        return failedQueues.isEmpty() && !hasFailed ? null : failedQueues;
    }

    private Map<String, Mode> changeLedgerSubscribe(MessageRouter router, MessageLedger ledger, String topic, Map<String, Mode> changedQueues) {
        int ledgerId = ledger.id();
        Map<Integer, Integer> ledgerVersions = topicLedgerVersions.get(topic);
        Integer oldVersion = ledgerVersions == null ? null : ledgerVersions.get(ledgerId);
        if (oldVersion != null && oldVersion > ledger.version()) {
            return changedQueues;
        }

        Map<Integer, Int2IntMap> ledgerMarkers = topicLedgerMarkers.get(topic);
        Int2IntMap markerCounts = ledgerMarkers == null ? null : ledgerMarkers.get(ledgerId);

        if (markerCounts == null) {
            if (changedQueues.isEmpty()) {
                return null;
            }

            return resetLedgerSubscribe(router, ledger, topic, changedQueues, null);
        }

        if (markerCounts.isEmpty()) {
            if (changedQueues.isEmpty()) {
                cleanLedgerSubscribe(topic, ledgerId);
                return null;
            }

            return resetLedgerSubscribe(router, ledger, topic, changedQueues, markerCounts);
        }

        if (oldVersion != null && oldVersion < ledger.version()) {
            return resetLedgerSubscribe(router, ledger, topic, changedQueues, markerCounts);
        }

        ClientChannel channel = ledgerChannels.get(ledgerId);
        if (channel == null || !channel.isActive() ||
                (!ledger.replicas().isEmpty() && !ledger.replicas().contains(channel.address()))) {
            return resetLedgerSubscribe(router, ledger, topic, changedQueues, markerCounts);
        }

        return changedQueues.isEmpty() ? null : alterLedgerSubscribe(router, ledger, topic, changedQueues, markerCounts);
    }

    private void cleanLedgerSubscribe(String topic, int ledgerId) {
        ClientChannel channel = ledgerChannels.get(ledgerId);
        if (channel != null && channel.isActive()) {
            doCleanSubscribe(channel, topic, ledgerId);
        }

        ledgerChannels.remove(ledgerId);
        ledgerSequences.remove(ledgerId);
        Map<Integer, Int2IntMap> ledgerMarkers = topicLedgerMarkers.get(topic);
        if (ledgerMarkers != null) {
            ledgerMarkers.remove(ledgerId);
        }

        if (ledgerMarkers == null || ledgerMarkers.isEmpty()) {
            topicLedgerMarkers.remove(topic);
            topicLedgerVersions.remove(topic);
            topicTokens.remove(topic);
        }
    }

    private Map<String, Mode> resetLedgerSubscribe(MessageRouter router, MessageLedger ledger, String topic, Map<String, Mode> changedQueues, Int2IntMap markerCounts) {
        int ledgerId = ledger.id();
        Int2IntMap newMarkerCounts = markerCounts == null
                ? new Int2IntOpenHashMap(changedQueues.size())
                : new Int2IntOpenHashMap(markerCounts);

        for (Map.Entry<String, Mode> entry : changedQueues.entrySet()) {
            int marker = router.calculateMarker(entry.getKey());
            Mode mode = entry.getValue();
            if (mode == Mode.APPEND) {
                newMarkerCounts.mergeInt(marker, 1, Integer::sum);
            } else if (mode == Mode.DELETE) {
                newMarkerCounts.mergeInt(marker, -1, Integer::sum);
            }
        }

        newMarkerCounts.int2IntEntrySet().removeIf(entry -> entry.getIntValue() <= 0);
        if (newMarkerCounts.isEmpty()) {
            cleanLedgerSubscribe(topic, ledgerId);
            return null;
        }

        ClientChannel channel = ledgerChannels.get(ledgerId);
        if (channel != null && channel.isActive()) {
            doCleanSubscribe(channel, topic, ledgerId);
        }
        ledgerChannels.remove(ledgerId);
        channel = newLedgerChannel(topic, ledger, channel);
        if (channel == null || !channel.isActive()) {
            return changedQueues;
        }

        ByteString markers = generateMarkers(newMarkerCounts.keySet());
        if (markers == null) {
            return changedQueues;
        }

        int epoch = -1;
        long index = -1;
        AtomicReference<MessageId> sequence = ledgerSequences.get(ledgerId);
        if (sequence != null) {
            if (markerCounts != null) {
                MessageId lastId = sequence.get();
                if (lastId != null) {
                    epoch = lastId.epoch();
                    index = lastId.index();
                }
            }
        } else {
            ledgerSequences.put(ledgerId, new AtomicReference<>());
        }

        ledgerChannels.put(ledgerId, channel);
        if (!doResetSubscribe(channel, topic, ledgerId, epoch, index, markers)) {
            doCleanSubscribe(channel, topic, ledgerId);
            ledgerChannels.remove(ledgerId);
            return changedQueues;
        }

        topicLedgerVersions.computeIfAbsent(topic, k -> new HashMap<>()).put(ledgerId, ledger.version());
        topicLedgerMarkers.computeIfAbsent(topic, k -> new HashMap<>()).put(ledgerId, markerCounts);
        return null;
    }

    private Map<String, Mode> alterLedgerSubscribe(MessageRouter router, MessageLedger ledger, String topic, Map<String, Mode> changedQueues, Int2IntMap markerCounts) {
        int ledgerId = ledger.id();

        IntSet alterSet = new IntOpenHashSet(changedQueues.size());
        Int2IntMap newMarkerCounts = new Int2IntOpenHashMap(markerCounts);

        for (Map.Entry<String, Mode> entry : changedQueues.entrySet()) {
            int marker = router.calculateMarker(entry.getKey());
            alterSet.add(marker);

            Mode mode = entry.getValue();
            if (mode == Mode.APPEND) {
                newMarkerCounts.mergeInt(marker, 1, Integer::sum);
            } else if (mode == Mode.DELETE) {
                newMarkerCounts.mergeInt(marker, -1, Integer::sum);
            }
        }

        newMarkerCounts.int2IntEntrySet().removeIf(entry -> entry.getIntValue() <= 0);
        if (newMarkerCounts.isEmpty()) {
            cleanLedgerSubscribe(topic, ledgerId);
            return null;
        }

        ClientChannel channel = ledgerChannels.get(ledgerId);
        if (channel == null || !channel.isActive()) {
            return changedQueues;
        }

        IntSet appendSet = new IntOpenHashSet();
        IntSet deleteSet = new IntOpenHashSet();
        for (int marker : alterSet) {
            int count = newMarkerCounts.get(marker);
            if (count > 0) {
                appendSet.add(marker);
            } else {
                deleteSet.add(marker);
            }
        }

        ByteString appendMarkers = generateMarkers(appendSet);
        ByteString deleteMarkers = generateMarkers(deleteSet);
        if (appendMarkers == null || deleteMarkers == null) {
            return changedQueues;
        }

        if (!doAlterSubscribe(channel, topic, ledgerId, appendMarkers, deleteMarkers)) {
            return resetLedgerSubscribe(router, ledger, topic, changedQueues, markerCounts);
        }

        topicLedgerVersions.computeIfAbsent(topic, k -> new HashMap<>()).put(ledgerId, ledger.version());
        topicLedgerMarkers.computeIfAbsent(topic, k -> new HashMap<>()).put(ledgerId, markerCounts);
        return null;
    }

    private boolean doResetSubscribe(ClientChannel channel, String topic, int ledgerId, int epoch, long index, ByteString markers) {
        try {
            Promise<ResetSubscribeResponse> promise = ImmediateEventExecutor.INSTANCE.newPromise();
            ResetSubscribeRequest request = ResetSubscribeRequest.newBuilder()
                    .setLedger(ledgerId)
                    .setEpoch(epoch)
                    .setIndex(index)
                    .setMarkers(markers)
                    .setTopic(topic)
                    .build();

            int timeoutMs = consumerConfig.getControlTimeoutMs();
            channel.invoker().resetSubscribe(timeoutMs, promise, request);
            promise.get(timeoutMs, TimeUnit.MILLISECONDS);
            return true;
        } catch (Throwable t) {
            return false;
        }
    }

    private boolean doAlterSubscribe(ClientChannel channel, String topic, int ledgerId, ByteString appendMarkers, ByteString deleteMarkers) {
        try {
            AlterSubscribeRequest request = AlterSubscribeRequest.newBuilder()
                    .setLedger(ledgerId)
                    .setTopic(topic)
                    .setAppendMarkers(appendMarkers)
                    .setDeleteMarkers(deleteMarkers)
                    .build();

            Promise<AlterSubscribeResponse> promise = ImmediateEventExecutor.INSTANCE.newPromise();
            int timeoutMs = consumerConfig.getControlTimeoutMs();
            channel.invoker().alterSubscribe(timeoutMs, promise, request);
            promise.get(timeoutMs, TimeUnit.MILLISECONDS);
            return true;
        } catch (Throwable t) {
            return false;
        }
    }

    private boolean doCleanSubscribe(ClientChannel channel, String topic, int ledgerId) {
        try {
            Promise<CleanSubscribeResponse> promise = ImmediateEventExecutor.INSTANCE.newPromise();
            CleanSubscribeRequest request = CleanSubscribeRequest.newBuilder()
                    .setLedger(ledgerId)
                    .setTopic(topic)
                    .build();

            int timeoutMs = consumerConfig.getControlTimeoutMs();
            channel.invoker().cleanSubscribe(timeoutMs, promise, request);
            promise.get(timeoutMs, TimeUnit.MILLISECONDS);
            return true;
        } catch (Throwable t) {
            return false;
        }
    }

    private ClientChannel newLedgerChannel(String topic, MessageLedger ledger, ClientChannel channel) {
        SocketAddress leader = ledger.leader();
        if (leader != null) {
            ClientChannel clientChannel = fetchChannel(leader);
            if (clientChannel != null && clientChannel.isActive()) {
                return clientChannel;
            }
        }

        for (SocketAddress address : ledger.replicas()) {
            ClientChannel clientChannel = fetchChannel(address);
            if (clientChannel != null && clientChannel.isActive()) {
                return clientChannel;
            }
        }

        if (ledger.replicas().isEmpty() && (channel != null && channel.isActive())) {
            return channel;
        }

        return null;
    }

    private ClientChannel fetchChannel(SocketAddress address) {
        try {
            return client.fetchChannel(address);
        } catch (Throwable t) {
            return null;
        }
    }

    private MessageRouter fetchRouter(String topic) {
        try {
            return client.fetchMessageRouter(topic);
        } catch (Throwable t) {
            return null;
        }
    }

    private MessageLedger calculateLedger(MessageRouter router, String queue) {
        try {
            return router.calculateLedger(queue);
        } catch (Throwable t) {
            return null;
        }
    }

    private ByteString generateMarkers(IntSet markers) {
        try {
            if (markers.isEmpty()) {
                return ByteString.EMPTY;
            }

            byte[] data = new byte[markers.size() * 4];
            CodedOutputStream stream = CodedOutputStream.newInstance(data);
            for (int marker : markers) {
                stream.writeFixed32NoTag(marker);
            }

            return UnsafeByteOperations.unsafeWrap(data);
        } catch (Throwable t) {
            return null;
        }
    }

    private Map<String, Integer> extractChangedRecords(Map<String, Map<String, Mode>> changedRecords) {
        Map<String, Integer> topicQueueCount = new HashMap<>();
        synchronized (wholeQueueTopics) {
            Iterator<Map.Entry<String, Map<String, Mode>>> queueIterator = wholeQueueTopics.entrySet().iterator();
            while (queueIterator.hasNext()) {
                Map.Entry<String, Map<String, Mode>> queueEntry = queueIterator.next();
                String queue = queueEntry.getKey();
                Map<String, Mode> topicModes = queueEntry.getValue();

                Iterator<Map.Entry<String, Mode>> topicIterator = topicModes.entrySet().iterator();
                while (topicIterator.hasNext()) {
                    Map.Entry<String, Mode> topicEntry = topicIterator.next();
                    String topic = topicEntry.getKey();
                    Map<String, Mode> changedQueues = changedRecords.computeIfAbsent(topic, k -> new HashMap<>());

                    Mode mode = topicEntry.getValue();
                    if (mode == Mode.APPEND) {
                        topicEntry.setValue(Mode.REMAIN);
                        topicQueueCount.compute(topic, (k, v) -> v == null ? 1 : v + 1);
                    } else if (mode == Mode.DELETE) {
                        topicIterator.remove();
                        topicQueueCount.putIfAbsent(topic, 0);
                    } else {
                        topicQueueCount.compute(topic, (k, v) -> v == null ? 1 : v + 1);
                        continue;
                    }

                    Mode oldMode = changedQueues.get(queue);
                    if (oldMode == null) {
                        changedQueues.put(queue, mode);
                    } else if (mode != oldMode) {
                        changedQueues.put(queue, null);
                    }
                }

                if (topicModes.isEmpty()) {
                    queueIterator.remove();
                }
            }
        }

        return topicQueueCount;
    }

    private Set<String> dumpPresentTopic(String topic) {
        Set<String> queues = new HashSet<>();
        synchronized (wholeQueueTopics) {
            Iterator<Map.Entry<String, Map<String, Mode>>> iterator = wholeQueueTopics.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Map<String, Mode>> entry = iterator.next();
                String queue = entry.getKey();
                Map<String, Mode> topicModes = entry.getValue();
                Mode mode = topicModes.get(topic);
                if (mode == Mode.APPEND) {
                    topicModes.put(topic, Mode.REMAIN);
                    queues.add(queue);
                } else if (mode == Mode.DELETE) {
                    topicModes.remove(topic);
                } else if (mode == Mode.REMAIN) {
                    queues.add(queue);
                }

                if (topicModes.isEmpty()) {
                    iterator.remove();
                }
            }
        }
        return queues;
    }

    private void scheduleFailedRetryTask(int delayMs) {
        if (failedRetryFuture != null || executor.isShuttingDown()) {
            return;
        }

        failedRetryFuture = executor.schedule(() -> {
            failedRetryFuture = null;
            touchChangedTask();
        }, delayMs, TimeUnit.MILLISECONDS);
    }

    private Map<String, Map<String, Mode>> clearFailedRecords() {
        Map<String, Map<String, Mode>> dumpFailedRecords = failedRecords;
        failedRecords = new HashMap<>();
        return dumpFailedRecords;
    }

    private void resetFailedRecords(String topic, Map<String, Mode> failedQueues) {
        if (failedQueues == null) {
            failedRecords.remove(topic);
        } else {
            failedRecords.put(topic, failedQueues);
        }
    }

    private boolean existFailedRecords() {
        return !failedRecords.isEmpty();
    }

    public synchronized void close() throws InterruptedException {
        if (state != Boolean.TRUE) {
            logger.warn("This consumer<{}> was closed", name);
            return;
        }

        state = Boolean.FALSE;

        if (executor != null) {
            executor.shutdownGracefully();
           try {
               while (!executor.isTerminated()) {
                   executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
               }
           } catch (Exception e) {
               Thread.currentThread().interrupt();
           }
        }

        if (group != null) {
            group.shutdownGracefully().sync();
            Iterator<EventExecutor> iterator = group.iterator();
            while (iterator.hasNext()) {
                try {
                    EventExecutor next = iterator.next();
                    while (! next.isTerminated()) {
                        next.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
                    }
                } catch (Exception e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        client.close();
    }

    enum Mode {
        REMAIN, APPEND, DELETE
    }

    private class MessageHandler {
        private final String id;
        private final Semaphore semaphore;
        private final EventExecutor executor;

        public MessageHandler(String id, Semaphore semaphore, EventExecutor executor) {
            this.id = id;
            this.semaphore = semaphore;
            this.executor = executor;
        }

        void handleMessage(ClientChannel channel, int marker, MessageId messageId, ByteBuf data) {
            if (executor.isShuttingDown()) {
                return;
            }

            semaphore.acquireUninterruptibly();
            data.retain();

            try {
                executor.execute((AbstractEventExecutor.LazyRunnable) () -> doHandleMessage(channel, marker, messageId, data));
            } catch (Throwable t) {
                data.release();
                semaphore.release();
                logger.error("Consumer<{}> handle message failed", id, t);
            }
        }

        private void doHandleMessage(ClientChannel channel, int marker, MessageId messageId, ByteBuf data) {
            int length = data.readableBytes();
            try {
                MessageMetadata metadata = ProtoBufUtils.readProto(data, MessageMetadata.parser());
                String topic = metadata.getTopic();
                String queue = metadata.getQueue();
                Map<String, Mode> topicModes = wholeQueueTopics.get(queue);
                Mode mode = topicModes == null ? null : topicModes.get(topic);
                if (mode == null || mode == Mode.DELETE) {
                    return;
                }

                Extras extras = new Extras(metadata.getExtrasMap());
                listener.onMessage(topic, queue, messageId, data, extras);
            } catch (Throwable t) {
                logger.error(" Consumer<{}> handle message failed, address={} marker={} messageId={} length={}",
                        name, channel.address(), marker, messageId, length, t);
            } finally {
                data.release();
                semaphore.release();
            }
        }
    }

    private class ConsumerListener implements ClientListener {
        @Override
        public void onChannelClosed(ClientChannel channel) {
            touchChangedTask();
        }

        @Override
        public void onTopicChanged(ClientChannel channel, TopicChangedSignal signal) {
            String topic = signal.getTopic();
            if (!client.containsMessageRouter(topic)) {
                logger.debug("The client<{}> doesn't contains topic<{}> message router", name, topic);
                return;
            }

            int ledgerId = signal.getLedger();
            int ledgerVersion = signal.getLedgerVersion();
            executor.schedule(() -> {
                try {
                    if (client.containsMessageRouter(topic)) {
                        MessageRouter router = client.fetchMessageRouter(topic);
                        if (router == null) {
                            logger.warn("The client<{}> topic<{}> message router is empty", name, topic);
                            return;
                        }

                        MessageLedger ledger = router.ledger(ledgerId);
                        if (ledger == null || ledgerVersion == 0 || ledger.version() < ledgerVersion) {
                            client.refreshMessageRouter(topic, channel);
                        }

                        touchChangedTask();
                    }
                } catch (Throwable t) {
                    logger.error(t);
                }
            }, ThreadLocalRandom.current().nextInt(5000), TimeUnit.MILLISECONDS);
        }

        @Override
        public void onPushMessage(ClientChannel channel, MessagePushSignal signal, ByteBuf data) {
            int ledger = signal.getLedger();
            int marker = signal.getMarker();
            int epoch = signal.getEpoch();
            long index = signal.getIndex();
            if (!channel.equals(ledgerChannels.get(ledger))) {
                try {
                    MessageMetadata metadata = ProtoBufUtils.readProto(data, MessageMetadata.parser());
                    String topic = metadata.getTopic();
                    obsoleteFutures.computeIfAbsent(ledger + "@" + channel.id(),
                            k -> executor.schedule(() -> {
                                if (channel.isActive() && !channel.equals(ledgerChannels.get(ledger))) {
                                    doCleanSubscribe(channel, topic, ledger);
                                }

                                obsoleteFutures.remove(k);
                            }, consumerConfig.getControlRetryDelayMs(), TimeUnit.MILLISECONDS));
                } catch (Throwable t) {
                    logger.error(t);
                }
                return;
            }

            AtomicReference<MessageId> sequence = ledgerSequences.get(ledger);
            if (sequence == null) {
                return;
            }

            MessageId id = new MessageId(ledger, epoch, index);
            while (true) {
                MessageId lastId = sequence.get();
                if (lastId == null || (epoch == lastId.epoch() && index > lastId.index() || epoch > lastId.epoch())) {
                    if (sequence.compareAndSet(lastId, id)) {
                        break;
                    }
                } else {
                    return;
                }
            }

            try {
                MessageHandler handler = handlers[consistentHash(marker, handlers.length)];
                handler.handleMessage(channel, marker, id, data);
            } catch (Throwable t) {
                logger.error("The client<{}> handle message failure, {}", name, t);
            }
        }

        private int consistentHash(int input, int buckets) {
            long state = input & 0xffffffffL;
            int candidate = 0;
            int next;
            while (true) {
                next = (int) ((candidate + 1) / (((double) ((int) ((state = (2862933555777941757L * state + 1)) >>> 33) + 1)) / 0x1.0p31));
                if (next > 0 && next < buckets) {
                    candidate = next;
                } else {
                    return candidate;
                }
            }
        }

        @Override
        public void onNodeOffline(ClientChannel channel, NodeOfflineSignal signal) {
            ClientListener.super.onNodeOffline(channel, signal);
        }
    }
}
