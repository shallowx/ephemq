package org.meteor.client.consumer;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.UnsafeByteOperations;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.meteor.client.core.Client;
import org.meteor.client.core.ClientChannel;
import org.meteor.client.core.CombineListener;
import org.meteor.client.core.MessageLedger;
import org.meteor.client.core.MessageRouter;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.common.message.MessageId;
import org.meteor.common.thread.FastEventExecutor;
import org.meteor.common.util.TopicPatternUtil;
import org.meteor.remote.proto.server.AlterSubscribeRequest;
import org.meteor.remote.proto.server.AlterSubscribeResponse;
import org.meteor.remote.proto.server.CleanSubscribeRequest;
import org.meteor.remote.proto.server.CleanSubscribeResponse;
import org.meteor.remote.proto.server.ResetSubscribeRequest;
import org.meteor.remote.proto.server.ResetSubscribeResponse;

/**
 * The DefaultConsumer class is an implementation of the Consumer interface,
 * providing functionalities to consume messages from specific topics and queues
 * within a messaging system. It manages message subscription, consumption,
 * and handles various states of the consumer lifecycle.
 */
public class DefaultConsumer implements Consumer {
    /**
     * Logger for the DefaultConsumer class to log internal events and actions.
     * It uses the InternalLogger from the InternalLoggerFactory specific to the DefaultConsumer class.
     */
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(DefaultConsumer.class);
    /**
     * The name of the DefaultConsumer instance. This variable holds the unique identifier or name
     * assigned to a particular instance of DefaultConsumer. It is a final variable, which means its
     * value is assigned once and cannot be changed.
     */
    private final String name;
    /**
     * Holds the configuration settings for the consumer.
     * <p>
     * This variable contains various configurations such as socket settings,
     * connection timeouts, worker thread limits, and more. It is used by the consumer
     * to communicate with backend services and perform its operations based on the
     * specified configuration.
     */
    private final ConsumerConfig consumerConfig;
    /**
     * The `Client` object used by the DefaultConsumer for interacting with the backend
     * or external services. It handles the underlying network communications and operations.
     */
    private final Client client;
    /**
     * Holds the subscription state of the consumer where the key is the topic, the inner map key represents queue names associated
     * with that topic, and the inner map value denotes the subscription mode (REMAIN, APPEND, or DELETE).
     *
     * This data structure is used to manage and track the various subscriptions the consumer has registered and their respective
     * states across different topics and queues.
     */
    private final Map<String, Map<String, Mode>> subscribeShips = new ConcurrentHashMap<>();
    /**
     * A thread-safe map holding ClientChannel instances indexed by their integer identifiers.
     * This map represents channels that are currently ready and is utilized for managing active connections
     * within the DefaultConsumer class.
     */
    private final Map<Integer, ClientChannel> readyChannels = new ConcurrentHashMap<>();
    /**
     * A map that maintains ledger sequences using AtomicReferences of MessageIds.
     * The keys are ledger identifiers (integers), and the values are AtomicReferences pointing
     * to the most recent MessageId for that ledger.
     *
     * This structure is used to track the latest message processed in each ledger, enabling
     * efficient message handling and ensuring thread-safe updates.
     */
    private final Map<Integer, AtomicReference<MessageId>> ledgerSequences = new ConcurrentHashMap<>();
    /**
     * An AtomicBoolean that indicates whether a change task has been touched or modified.
     *
     * This variable is used to manage and track the state of change-related tasks within the consumer's operations.
     * It helps in synchronizing tasks that involve changes to subscriptions, ledgers, or any other configurations
     * managed by the consumer.
     *
     * The AtomicBoolean ensures thread-safe operations and provides a non-blocking mechanism to check and update the
     * task's state, making it suitable for concurrent environments.
     *
     * Initialized to false, indicating no changes have been made initially.
     */
    private final AtomicBoolean changeTaskTouched = new AtomicBoolean(false);
    /**
     * A map that stores the number of tokens available for each topic.
     *
     * The key is the topic's name, and the value is the number of tokens available for that topic.
     * This is used to manage flow control, ensuring that each topic manages its tokens independently.
     */
    private final Map<String, Long> topicTokens = new HashMap<>();
    /**
     * Stores the version information of different ledgers for various topics.
     *
     * The outer map's key is a topic name, and the inner map's key is an integer representing the ledger ID.
     * The inner map's value is an integer representing the version number of the corresponding ledger.
     *
     * This data structure allows tracking of which ledger versions have been processed or are currently being
     * processed for each topic. It facilitates efficient management and validation of message consumption
     * from multiple ledgers within various topics by keeping a precise and organized record of their versions.
     */
    private final Map<String, Map<Integer, Integer>> topicLedgerVersions = new HashMap<>();
    /**
     * A mapping structure that holds marker information for topic ledgers.
     *
     * The outer map uses topic names as keys, and values are inner maps where
     * the keys represent ledger ID numbers and the values are mappings of
     * integer markers using an Int2IntMap. This structure is used to keep
     * track of markers associated with individual ledgers for different topics.
     */
    private final Map<String, Map<Integer, Int2IntMap>> topicLedgerMarkers = new HashMap<>();
    /**
     * A listener for a combined set of events occurring within the consumer's operation.
     * This listener integrates various event-handling functionalities such as channel
     * active or closed events, message push signals, topic changes, node offline signals,
     * and synchronization messages.
     *
     * This field holds a reference to the CombineListener implementation which will handle
     * these specific types of events. The listener ensures that the consumer can react to
     * and manage different operational states and messages efficiently.
     */
    private final CombineListener listener;
    /**
     * The executor responsible for executing tasks and callbacks associated
     * with the consumer's operations.
     *
     * This EventExecutor handles asynchronous tasks such as message
     * processing, subscription handling, and other internal operations
     * that require non-blocking execution. It ensures tasks are executed
     * in a timely manner, providing a means to offload work from the
     * main thread and improve the responsiveness and throughput of
     * the consumer.
     */
    private EventExecutor executor;
    /**
     * Indicates the current state of the DefaultConsumer. This volatile boolean helps manage the consumer's state
     * across multiple threads, providing visibility and atomicity guarantees. This is crucial in a multi-threaded
     * environment to ensure that changes to the state variable are immediately visible to other threads.
     */
    private volatile boolean state = false;
    /**
     * Represents a Future task that handles retries for failed operations in the DefaultConsumer class.
     * The Future object allows for executing retry logic asynchronously and potentially canceling
     * or querying the status of the retry attempts. This variable is used internally to manage retries
     * for operations that did not succeed on the first attempt.
     */
    private Future<?> failedRetryFuture;
    /**
     * A map that holds failed record attempts organized by topic and queue.
     * The outer map's key represents the topic name, while the inner map's key represents the queue name.
     * The inner map's value indicates the failure mode (e.g., REMAIN, APPEND, DELETE) for each queue.
     */
    private Map<String, Map<String, Mode>> failedRecords = new HashMap<>();

    /**
     * Constructs a new DefaultConsumer instance with the specified parameters.
     *
     * @param name the name of the consumer.
     * @param consumerConfig the configuration settings for the consumer.
     * @param messageListener the listener for handling incoming messages.
     */
    public DefaultConsumer(String name, ConsumerConfig consumerConfig, MessageListener messageListener) {
        this(name, consumerConfig, messageListener, null);
    }

    /**
     * Constructs a DefaultConsumer instance with the specified parameters.
     *
     * @param name            the name of the consumer
     * @param consumerConfig  the configuration settings for the consumer
     * @param messageListener the listener for message events
     * @param clientListener  the optional combined listener for additional configurations; if null, a default listener will be used
     */
    public DefaultConsumer(String name, ConsumerConfig consumerConfig, MessageListener messageListener, CombineListener clientListener) {
        this.name = name;
        this.consumerConfig = Objects.requireNonNull(consumerConfig, "Consumer config not found");
        this.listener = clientListener == null
                ? new DefaultConsumerListener(this, consumerConfig, Objects.requireNonNull(messageListener, "Consumer message listener not found"))
                : clientListener;
        this.client = new Client(name, consumerConfig.getClientConfig(), listener);
    }

    /**
     * Starts the consumer if it is not already running. This method is thread-safe and will ensure
     * that the consumer's state is updated, the client is started, and a scheduled task to handle
     * touch changes is initiated.
     *
     * If the consumer is already running, a warning will be logged indicating that the consumer
     * with the specified name is already started.
     *
     * The method performs the following steps:
     * 1. Checks if the consumer is already running.
     * 2. If running, logs a warning message.
     * 3. Sets the consumer state to true.
     * 4. Starts the client.
     * 5. Initializes an executor to handle consumer tasks.
     * 6. Schedules a fixed-delay task to handle touch changes every 30 seconds.
     */
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

    /**
     * Subscribes the consumer to specified message queues of various topics.
     * This method takes a map where the keys are topic names and the values
     * are the corresponding queue names. It sets up subscriptions for each
     * topic-queue pair provided in the map.
     *
     * @param ships a map where keys are topic names and values are queue names
     */
    @Override
    public void subscribe(Map<String, String> ships) {
        if (ships == null || ships.isEmpty()) {
            return;
        }
        for (Map.Entry<String, String> entry : ships.entrySet()) {
            try {
                this.subscribe(entry.getKey(), entry.getValue());
            } catch (Throwable t) {
                if (logger.isErrorEnabled()) {
                    logger.error(t.getMessage(), t);
                }
            }
        }
    }

    /**
     * Subscribes to a given topic and queue. If the topic already exists in a "DELETE" state, it is updated to "REMAIN".
     * Otherwise, the topic is added with the "APPEND" state.
     *
     * @param topic The topic to subscribe to.
     * @param queue The queue to associate with the subscription.
     * @throws NullPointerException if the topic or queue is null.
     * @throws IllegalStateException if the topic or queue does not meet validation criteria.
     */
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

    /**
     * Cancels the subscription for a given topic and queue. Validates the topic and queue,
     * updates the subscription status, and performs necessary internal adjustments.
     *
     * @param topic the topic name whose subscription needs to be canceled
     * @param queue the queue name associated with the subscription
     */
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

    /**
     * Clears the specified topic from the subscription management.
     *
     * @param topic the topic to be cleared from subscriptions
     * @throws NullPointerException if the topic is null
     * @throws IllegalStateException if the topic is invalid or exceeds the maximum length
     */
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

    /**
     * Initiates the execution of a task to handle changes if certain conditions are met.
     *
     * This method ensures that the change task is not executed if the executor
     * is shutting down, or if the change task has already been touched.
     * If the executor is operational and the change task is not presently being handled,
     * it is marked as touched, and a new task is submitted to the executor for processing.
     *
     * In case of failure during task submission, the touched state is reset and an error
     * message is logged.
     */
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

    /**
     * Performs several tasks related to managing the state of subscriptions and message routing.
     *
     * This method resets the change task indicator, clears failed records, and processes
     * changed records for subscribed topics. For each topic, it either cleans up the
     * subscription or attempts to change or reset it based on various conditions. If there
     * are any failed records after processing, a retry task is scheduled.
     *
     * Sequence of operations:
     * 1. Resets the change task indicator.
     * 2. Clears failed records and extracts changed records.
     * 3. If there are no changed records, the operation is terminated.
     * 4. Iterates over the changed records and handles each topic:
     *    - Cleans up the subscription if the queue count for the topic is zero.
     *    - Resets failed records if a suitable router is not found.
     *    - Updates topic tokens and processes the subscription change.
     * 5. Schedules a retry task if failed records still exist.
     */
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

            MessageRouter router = getRouter(topic);
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

    /**
     * Cleans up subscriptions related to a given topic.
     *
     * @param topic The topic for which to clean up subscriptions.
     */
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

    /**
     * Resets the subscription for the given topic and attempts to subscribe to the topic queues.
     * If the subscription fails for certain queues, it returns a map of these queues with reasons
     * for failure.
     *
     * @param router The MessageRouter instance used to get the ledgers and route markers.
     * @param topic  The topic for which to reset the subscription.
     * @return A map containing queues that failed to subscribe along with the corresponding {@link Mode},
     * or null if all subscriptions are successful.
     */
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
            MessageLedger ledger = computeMessageLedger(router, queue);
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

    /**
     * Changes the subscription state for the specified topic based on the given map of changed queues and their subscription modes.
     *
     * @param router The message router object used to route queues to their corresponding ledgers.
     * @param topic The topic for which the subscription state is to be changed.
     * @param changedQueues A map where the keys are queue names and the values are the intended subscription modes.
     *
     * @return A map of queues and their modes that failed to change subscription. Returns null if all subscriptions were successfully changed without any failures.
     */
    private Map<String, Mode> changeTopicSubscribe(MessageRouter router, String topic, Map<String, Mode> changedQueues) {
        Map<String, Mode> failedQueues = new HashMap<>();
        Map<MessageLedger, Map<String, Mode>> ledgerRecords = new HashMap<>();
        for (Map.Entry<String, Mode> entry : changedQueues.entrySet()) {
            String queue = entry.getKey();
            MessageLedger ledger = computeMessageLedger(router, queue);
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

    /**
     * Adjusts the subscription of a message ledger, potentially altering or resetting the ledger subscription
     * based on the provided conditions and the current state of the ledger.
     *
     * @param router         The message router used to handle the routing logic.
     * @param ledger         The message ledger whose subscription is to be changed.
     * @param topic          The topic associated with the ledger.
     * @param changedQueues  The map containing the queues and their respective subscription modes that need alteration.
     * @return A map representing the changed queues with their new modes, or null if no changes are made.
     */
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

    /**
     * Cleans and unsubscribes a ledger from a topic.
     *
     * @param topic    the topic from which to clean and unsubscribe the ledger
     * @param ledgerId the identifier of the ledger to be cleaned and unsubscribed
     */
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

    /**
     * Resets the subscription for a given ledger based on the provided marker counts and changed queues.
     *
     * @param router The message router used to determine marker routes.
     * @param ledger The message ledger associated with the subscription.
     * @param topic The topic associated with the subscription.
     * @param changedQueues A map of queues and their new modes (REMAIN, APPEND, DELETE).
     * @param markerCounts Current marker counts, can be null.
     * @return A map of changed queues if the reset operation failed, otherwise null.
     */
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

    /**
     * Alters the subscription of a ledger based on the provided `changedQueues` and updates the `markerCounts`.
     * It modifies the ledger's markers for APPEND and DELETE modes and adjusts the subscription accordingly.
     *
     * @param router        The {@link MessageRouter} instance used for routing messages.
     * @param ledger        The {@link MessageLedger} instance representing the ledger to be altered.
     * @param topic         The topic name associated with the ledger.
     * @param changedQueues A map containing the queues with their corresponding {@link Mode} to be changed.
     * @param markerCounts  An {@link Int2IntMap} containing the counts of markers for the ledger.
     * @return A map of the altered queues with their respective modes if the alteration is unsuccessful, otherwise null.
     */
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

    /**
     * Resets the subscription for a client channel to a specific topic and ledger position.
     *
     * @param channel The client channel object that represents the connection.
     * @param topic The name of the topic to reset the subscription to.
     * @param ledgerId The identifier of the ledger.
     * @param epoch The epoch number associated with the ledger.
     * @param index The index within the ledger.
     * @param markers Any byte string representing markers for the reset operation.
     * @return A boolean value indicating the success or failure of the reset operation. Returns {@code false} on success, {@code true} on failure.
     */
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

    /**
     * Alters the subscription for the given channel and topic.
     *
     * @param channel The {@code ClientChannel} object representing the channel.
     * @param topic The topic string for which the subscription needs to be altered.
     * @param ledgerId An integer representing the ledger identifier.
     * @param appendMarkers A {@code ByteString} representing the append markers to be added.
     * @param deleteMarkers A {@code ByteString} representing the delete markers to be removed.
     * @return A boolean value indicating the success of the operation. Returns true if the subscription alteration was successful, otherwise false.
     */
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

    /**
     * Performs a clean subscribe action on the given client channel and topic.
     *
     * @param channel the client channel used to perform the clean subscribe action
     * @param topic the topic to which the clean subscribe action will be applied
     * @param ledgerId the ledger identifier associated with the topic
     */
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

    /**
     * Creates a new ledger channel for the given MessageLedger. It attempts to find an active
     * ClientChannel from the ledger's leader or participants. If no leader or participants are
     * available, it returns the provided channel if it is active.
     *
     * @param ledger The MessageLedger containing the leader and participants.
     * @param channel The fallback ClientChannel to use if no leader or participant channels are found.
     * @return An active ClientChannel, or null if none can be found.
     */
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

    /**
     * Fetches an active client channel for the specified socket address.
     *
     * @param address the socket address for which to fetch the client channel
     * @return the active client channel associated with the specified address or null if fetching the channel fails
     */
    private ClientChannel fetchChannel(SocketAddress address) {
        try {
            return client.getActiveChannel(address);
        } catch (Throwable t) {
            if (logger.isDebugEnabled()) {
                logger.debug("Consumer[{}] fetch channel error, address[{}]", name, address.toString(), t.getMessage(), t);
            }
            return null;
        }
    }

    /**
     * Retrieves the MessageRouter instance for the specified topic.
     *
     * @param topic the topic for which the MessageRouter is to be fetched
     * @return the MessageRouter for the specified topic, or null if an error occurs
     */
    MessageRouter getRouter(String topic) {
        try {
            return client.fetchRouter(topic);
        } catch (Throwable t) {
            if (logger.isDebugEnabled()) {
                logger.debug("Consumer[{}] fetch router error, topic[{}]", name, topic, t.getMessage(), t);
            }
            return null;
        }
    }

    /**
     * Computes the MessageLedger for the given queue using the provided MessageRouter.
     *
     * @param router the MessageRouter used to compute the MessageLedger
     * @param queue the queue for which the MessageLedger is computed
     * @return the computed MessageLedger, or null in case of an error
     */
    private MessageLedger computeMessageLedger(MessageRouter router, String queue) {
        try {
            return router.routeLedger(queue);
        } catch (Throwable t) {
            if (logger.isDebugEnabled()) {
                logger.debug("Consumer[{}] calculate ledger error, topic[{}] queue[{}]", name, router.topic(), queue, t.getMessage(), t);
            }
            return null;
        }
    }

    /**
     * Generates a ByteString containing markers from the given IntSet.
     * Each marker is written as a fixed 32-bit integer with no tag.
     *
     * @param markers the set of markers to be converted into a ByteString
     * @return a ByteString containing the markers, or null if an error occurs
     */
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

    /**
     * Extracts the changed records from the provided data structure and updates the respective counts.
     *
     * @param changedRecords A map where the keys are topic names and the values are maps of queue names and their respective Mode (REMAIN, APPEND, DELETE).
     * @return A map where the keys are topic names and the values are the count of the queues in each topic.
     */
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

    /**
     * Dumps the present topic and updates its mode within the subscription ships structure.
     * Depending on the mode (REMAIN, APPEND, DELETE), it modifies or removes the topic from the subscription map.
     *
     * @param topic the topic whose queues and modes are to be dumped and updated.
     * @return a set of queue names that are associated with the provided topic.
     */
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

    /**
     * Schedules a retry task for handling failed operations after a specified delay. This is used to
     * attempt to recover from transient failures in processing or network operations.
     *
     * @param delayMs the delay in milliseconds before the retry task is executed.
     */
    private void scheduleFailedRetryTask(int delayMs) {
        if (failedRetryFuture != null || executor.isShuttingDown()) {
            return;
        }

        failedRetryFuture = executor.schedule(() -> {
            failedRetryFuture = null;
            touchChangedTask();
        }, delayMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Clears all failed records and returns the previous set of failed records.
     *
     * This method resets the failed records map to an empty state, effectively clearing all
     * previously stored failed records. The method is typically used in scenarios where
     * failed message processing records need to be reset, for instance, when retry operations
     * or cleanup tasks are initiated.
     *
     * @return A map containing the previously stored failed records, which associate topic names
     *         with another map that relates queue names to their respective modes.
     */
    private Map<String, Map<String, Mode>> clearFailedRecords() {
        Map<String, Map<String, Mode>> dumpFailedRecords = failedRecords;
        failedRecords = new HashMap<>();
        return dumpFailedRecords;
    }

    /**
     * Resets the failed records for a specified topic. If the provided map of failed queues is null,
     * the failed records for the topic will be removed. Otherwise, updates the failed records with
     * the provided map of failed queues.
     *
     * @param topic the topic whose failed records need to be reset
     * @param failedQueues a map of queues with their respective modes indicating the failure states;
     *                     if null, the failed records for the topic will be removed
     */
    private void resetFailedRecords(String topic, Map<String, Mode> failedQueues) {
        if (failedQueues == null) {
            failedRecords.remove(topic);
        } else {
            failedRecords.put(topic, failedQueues);
        }
    }

    /**
     * Checks if there are any failed records in the consumer.
     *
     * @return true if there are failed records, false otherwise
     */
    private boolean existFailedRecords() {
        return !failedRecords.isEmpty();
    }

    /**
     * Closes the consumer, terminating its operations and releasing associated resources.
     *
     * <ul>
     * <li>If the consumer is not currently running, a warning will be logged and the method will return immediately.
     * <li>Sets the consumer's state to false to indicate it is no longer running.
     * <li>Initiates a graceful shutdown of the executor if it is not null.
     * <li>Awaits the termination of the executor, handling any potential interruptions by restoring the thread's interrupted status.
     * <li>Invokes the listener's completion method to signal that the listener has finished its tasks.
     * <li>Closes the client to release any remaining resources associated with it.
     * </ul>
     *
     * @throws InterruptedException if the current thread is interrupted while waiting for the executor to terminate
     */
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

    /**
     * Checks if the consumer is currently running.
     *
     * @return true if the consumer is running, false otherwise
     */
    private boolean isRunning() {
        return state;
    }

    /**
     * Retrieves the name associated with this consumer.
     *
     * @return the name of the consumer
     */
    String getName() {
        return name;
    }

    /**
     * Returns the EventExecutor associated with this DefaultConsumer.
     *
     * @return the executor used for executing consumer-related tasks.
     */
    EventExecutor getExecutor() {
        return executor;
    }

    /**
     * Retrieves the current subscription mappings for this consumer instance.
     *
     * This method provides access to the internal subscription structure
     * used by the consumer. The returned map contains topic names as keys
     * and nested maps as values. Each nested map has queue names as keys
     * and the corresponding subscription mode as values.
     *
     * @return A map where the keys are topic names and the values are maps.
     *         Each inner map has queue names as keys and their associated
     *         subscription modes as values.
     */
    Map<String, Map<String, Mode>> getSubscribeShips() {
        return subscribeShips;
    }

    /**
     * Retrieves the map of ready channels.
     *
     * @return a map where the keys are integers representing ledger IDs
     *         and the values are {@link ClientChannel} instances that are ready.
     */
    Map<Integer, ClientChannel> getReadyChannels() {
        return readyChannels;
    }

    /**
     * Retrieves the ledger sequences, where each ledger is mapped to its corresponding sequence represented by an AtomicReference<MessageId>.
     *
     * @return a map where the keys are integer ledger IDs and the values are AtomicReference objects wrapping MessageId instances representing the current sequences of the ledgers
     * .
     */
    Map<Integer, AtomicReference<MessageId>> getLedgerSequences() {
        return ledgerSequences;
    }

    /**
     * Checks if the topic has an associated message router.
     *
     * @param topic the topic to be checked for an associated message router
     * @return true if the topic has an associated message router, false otherwise
     */
    boolean containsRouter(String topic) {
        return client.containsRouter(topic);
    }

    /**
     * Refreshes the message router for a given topic using the specified client channel.
     *
     * @param topic   the topic for which the router needs to be refreshed.
     * @param channel the client channel to be used for refreshing the router.
     */
    void refreshRouter(String topic, ClientChannel channel) {
        client.refreshRouter(topic, channel);
    }
}
