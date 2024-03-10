package org.meteor.client.consumer;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.UnsafeByteOperations;
import io.netty.util.concurrent.*;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.meteor.client.internal.*;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.common.message.MessageId;
import org.meteor.common.thread.FastEventExecutor;
import org.meteor.common.util.TopicPatternUtil;
import org.meteor.remote.proto.server.*;

import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class DefaultConsumer implements Consumer {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(DefaultConsumer.class);
    private final String name;
    private final ConsumerConfig consumerConfig;
    private final Client client;
    private final Map<String, Map<String, Mode>> subscribeShips = new ConcurrentHashMap<>();
    private final Map<Integer, ClientChannel> readyChannels = new ConcurrentHashMap<>();
    private final Map<Integer, AtomicReference<MessageId>> ledgerSequences = new ConcurrentHashMap<>();
    private final AtomicBoolean changeTaskTouched = new AtomicBoolean(false);
    private final Map<String, Long> topicTokens = new HashMap<>();
    private final Map<String, Map<Integer, Integer>> topicLedgerVersions = new HashMap<>();
    private final Map<String, Map<Integer, Int2IntMap>> topicLedgerMarkers = new HashMap<>();
    private EventExecutor executor;
    private volatile boolean state = false;
    private Future<?> failedRetryFuture;
    private Map<String, Map<String, Mode>> failedRecords = new HashMap<>();
    private final CombineListener listener;

    public DefaultConsumer(String name, ConsumerConfig consumerConfig, MessageListener messageListener) {
        this(name, consumerConfig, messageListener, null);
    }

    public DefaultConsumer(String name, ConsumerConfig consumerConfig, MessageListener messageListener, CombineListener clientListener) {
        this.name = name;
        this.consumerConfig = Objects.requireNonNull(consumerConfig, "Consumer config not found");
        this.listener = clientListener == null
                ? new DefaultConsumerListener(this, consumerConfig, Objects.requireNonNull(messageListener, "Consumer message listener not found"))
                : clientListener;
        this.client = new Client(name, consumerConfig.getClientConfig(), listener);
    }

    @Override
    public synchronized void start() {
        if (isRunning()) {
            if (logger.isWarnEnabled()) {
                logger.warn("The Consumer[{}] client[{}] war started", name, name);
            }
            return;
        }

        state = true;
        client.start();
        executor = new FastEventExecutor(new DefaultThreadFactory("consumer-task"));
        executor.scheduleWithFixedDelay(this::touchChangedTask, 30, 30, TimeUnit.SECONDS);
    }

    @Override
    public void subscribe(Map<String, String> ships) {
        if (ships == null || ships.isEmpty()) {
            return;
        }
        for (Map.Entry<String, String> entry : ships.entrySet()) {
            try {
                this.subscribe(entry.getKey(), entry.getValue());
            } catch (Throwable t) {
                if (logger.isErrorEnabled()){
                    logger.error(t.getMessage(), t);
                }
            }
        }
    }

    @Override
    public void subscribe(String topic, String queue) {
        TopicPatternUtil.validateTopic(topic);
        TopicPatternUtil.validateQueue(queue);

        topic = topic.intern();
        synchronized (subscribeShips) {
            Map<String, Mode> topicModes = subscribeShips.computeIfAbsent(queue, k -> new ConcurrentHashMap<>());
            Mode mode = topicModes.get(topic);
            if (mode == null) {
                topicModes.put(topic, Mode.APPEND);
            } else if (mode == Mode.DELETE) {
                topicModes.put(topic, Mode.REMAIN);
            } else {
                return;
            }
        }

        touchChangedTask();
    }

    public void cancelSubscribe(String topic, String queue) {
        TopicPatternUtil.validateTopic(topic);
        TopicPatternUtil.validateQueue(queue);

        topic = topic.intern();
        synchronized (subscribeShips) {
            Map<String, Mode> topicModes = subscribeShips.get(queue);
            if (topicModes == null) {
                return;
            }

            Mode mode = topicModes.get(topic);
            if (mode == Mode.REMAIN) {
                topicModes.put(topic, Mode.DELETE);
            } else if (mode == Mode.APPEND) {
                topicModes.remove(topic);
            } else {
                return;
            }
            if (topicModes.isEmpty()) {
                subscribeShips.remove(queue);
            }
        }
        touchChangedTask();
    }

    @Override
    public void clear(String topic) {
        TopicPatternUtil.validateTopic(topic);

        topic = topic.intern();
        boolean cleared = false;
        synchronized (subscribeShips) {
            Iterator<Map.Entry<String, Map<String, Mode>>> iterator = subscribeShips.entrySet().iterator();
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
    }

    void touchChangedTask() {
        if (executor.isShuttingDown() || !changeTaskTouched.compareAndSet(false, true)) {
            return;
        }

        try {
            executor.execute(this::doChangeTask);
        } catch (Throwable t) {
            changeTaskTouched.set(false);
            if (logger.isErrorEnabled()) {
                logger.error("Consumer[{}] touch changed task execute failed, and trg again later", name);
            }
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
            scheduleFailedRetryTask(consumerConfig.getControlRetryDelayMilliseconds());
        }
    }

    private void cleanTopicSubscribe(String topic) {
        Map<Integer, Int2IntMap> ledgerMarkers = topicLedgerMarkers.get(topic);
        if (ledgerMarkers != null) {
            for (int ledgerId : ledgerMarkers.keySet()) {
                ClientChannel channel = readyChannels.get(ledgerId);
                if (channel != null && channel.isActive()) {
                    doCleanSubscribe(channel, topic, ledgerId);
                }

                readyChannels.remove(ledgerId);
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

            ClientChannel channel = readyChannels.get(ledgerId);
            if (channel != null && channel.isActive()) {
                doCleanSubscribe(channel, topic, ledgerId);
            }

            readyChannels.remove(ledgerId);
            channel = newLedgerChannel(ledger, channel);
            if (channel == null || !channel.isActive()) {
                queues.forEach(q -> failedQueues.put(q, Mode.APPEND));
                continue;
            }

            Int2IntMap markersCounts = new Int2IntOpenHashMap(queues.size());
            for (String queue : queues) {
                int marker = router.routeMarker(queue);
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

            readyChannels.put(ledgerId, channel);
            if (doResetSubscribe(channel, topic, ledger.id(), epoch, index, markers)) {
                doCleanSubscribe(channel, topic, ledger.id());
                readyChannels.remove(ledgerId);
                queues.forEach(q -> failedQueues.put(q, Mode.APPEND));
                continue;
            }

            newLedgerVersions.put(ledgerId, ledger.version());
            newLedgerMarkers.put(ledgerId, markersCounts);
        }

        for (int ledgerId : oldLedgerMarkers.keySet()) {
            if (!newLedgerMarkers.containsKey(ledgerId)) {
                ClientChannel channel = readyChannels.get(ledgerId);
                if (channel != null && channel.isActive()) {
                    doCleanSubscribe(channel, topic, ledgerId);
                }

                readyChannels.remove(ledgerId);
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

        ClientChannel channel = readyChannels.get(ledgerId);
        if (channel == null || !channel.isActive() ||
                (!ledger.participants().isEmpty() && !ledger.participants().contains(channel.address()))) {
            return resetLedgerSubscribe(router, ledger, topic, changedQueues, markerCounts);
        }

        return changedQueues.isEmpty() ? null : alterLedgerSubscribe(router, ledger, topic, changedQueues, markerCounts);
    }

    private void cleanLedgerSubscribe(String topic, int ledgerId) {
        ClientChannel channel = readyChannels.get(ledgerId);
        if (channel != null && channel.isActive()) {
            doCleanSubscribe(channel, topic, ledgerId);
        }

        readyChannels.remove(ledgerId);
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
            int marker = router.routeMarker(entry.getKey());
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

        ClientChannel channel = readyChannels.get(ledgerId);
        if (channel != null && channel.isActive()) {
            doCleanSubscribe(channel, topic, ledgerId);
        }
        readyChannels.remove(ledgerId);
        channel = newLedgerChannel(ledger, channel);
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

        readyChannels.put(ledgerId, channel);
        if (doResetSubscribe(channel, topic, ledgerId, epoch, index, markers)) {
            doCleanSubscribe(channel, topic, ledgerId);
            readyChannels.remove(ledgerId);
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
            int marker = router.routeMarker(entry.getKey());
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

        ClientChannel channel = readyChannels.get(ledgerId);
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

            int timeoutMs = consumerConfig.getControlTimeoutMilliseconds();
            channel.invoker().resetSubscribe(timeoutMs, promise, request);
            promise.get(timeoutMs, TimeUnit.MILLISECONDS);
            return false;
        } catch (Throwable t) {
            if (logger.isDebugEnabled()) {
                logger.debug("Consumer[{}] reset subscribe error, topic[{}] channel[{}] ledger_id[{}] epoch[{}] index[{}] markers[{}]",
                        name, topic, channel, ledgerId, epoch, index, markers, t.getMessage(), t);
            }
            return true;
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
            int timeoutMs = consumerConfig.getControlTimeoutMilliseconds();
            channel.invoker().alterSubscribe(timeoutMs, promise, request);
            promise.get(timeoutMs, TimeUnit.MILLISECONDS);
            return true;
        } catch (Throwable t) {
            if (logger.isDebugEnabled()) {
                logger.debug("Consumer[{}] alter subscribe error, topic[{}] channel[{}] ledger_id[{}] append_markers[{}] delete_markers[{}]",
                        name, topic, channel, ledgerId, appendMarkers, deleteMarkers, t.getMessage(), t);
            }
            return false;
        }
    }

    void doCleanSubscribe(ClientChannel channel, String topic, int ledgerId) {
        try {
            Promise<CleanSubscribeResponse> promise = ImmediateEventExecutor.INSTANCE.newPromise();
            CleanSubscribeRequest request = CleanSubscribeRequest.newBuilder()
                    .setLedger(ledgerId)
                    .setTopic(topic)
                    .build();

            int timeoutMs = consumerConfig.getControlTimeoutMilliseconds();
            channel.invoker().cleanSubscribe(timeoutMs, promise, request);
            promise.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (Throwable t) {
            logger.debug(t.getMessage(), t);
        }
    }

    private ClientChannel newLedgerChannel(MessageLedger ledger, ClientChannel channel) {
        SocketAddress leader = ledger.leader();
        if (leader != null) {
            ClientChannel clientChannel = fetchChannel(leader);
            if (clientChannel != null && clientChannel.isActive()) {
                return clientChannel;
            }
        }

        for (SocketAddress address : ledger.participants()) {
            ClientChannel clientChannel = fetchChannel(address);
            if (clientChannel != null && clientChannel.isActive()) {
                return clientChannel;
            }
        }

        if (ledger.participants().isEmpty() && (channel != null && channel.isActive())) {
            return channel;
        }
        return null;
    }

    private ClientChannel fetchChannel(SocketAddress address) {
        try {
            return client.fetchChannel(address);
        } catch (Throwable t) {
            if (logger.isDebugEnabled()) {
                logger.debug("Consumer[{}] fetch channel error, address[{}]", name, address.toString(), t.getMessage(), t);
            }
            return null;
        }
    }

    MessageRouter fetchRouter(String topic) {
        try {
            return client.fetchRouter(topic);
        } catch (Throwable t) {
            if (logger.isDebugEnabled()) {
                logger.debug("Consumer[{}] fetch router error, topic[{}]", name, topic, t.getMessage(), t);
            }
            return null;
        }
    }

    private MessageLedger calculateLedger(MessageRouter router, String queue) {
        try {
            return router.routeLedger(queue);
        } catch (Throwable t) {
            if (logger.isDebugEnabled()) {
                logger.debug("Consumer[{}] calculate ledger error, topic[{}] queue[{}]", name, router.topic(), queue, t.getMessage(), t);
            }
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
            if (logger.isDebugEnabled()) {
                logger.debug("Consumer[{}] generate markers error, markers[{}]", name, markers, t.getMessage(), t);
            }
            return null;
        }
    }

    private Map<String, Integer> extractChangedRecords(Map<String, Map<String, Mode>> changedRecords) {
        Map<String, Integer> topicQueueCount = new HashMap<>();
        synchronized (subscribeShips) {
            Iterator<Map.Entry<String, Map<String, Mode>>> queueIterator = subscribeShips.entrySet().iterator();
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
        synchronized (subscribeShips) {
            Iterator<Map.Entry<String, Map<String, Mode>>> iterator = subscribeShips.entrySet().iterator();
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
                    if (topicModes.isEmpty()) {
                        iterator.remove();
                    }
                } else if (mode == Mode.REMAIN) {
                    queues.add(queue);
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

    @Override
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public synchronized void close() throws InterruptedException {
        if (!isRunning()) {
            if (logger.isWarnEnabled()) {
                logger.warn("The consumer[{}] was closed, don't execute it replay", name);
            }
            return;
        }
        state = false;
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
        listener.listenerCompleted();
        client.close();
    }

    private boolean isRunning() {
        return state;
    }

    String getName() { return name;}
    EventExecutor getExecutor() { return executor;}
    Map<String, Map<String, Mode>> getSubscribeShips() {
        return subscribeShips;
    }
    Map<Integer, ClientChannel> getReadyChannels() {
        return readyChannels;
    }
    Map<Integer, AtomicReference<MessageId>> getLedgerSequences() {
        return ledgerSequences;
    }
    boolean containsRouter(String topic) {
        return client.containsRouter(topic);
    }
    void refreshRouter(String topic, ClientChannel channel) {
        client.refreshRouter(topic, channel);
    }
}
