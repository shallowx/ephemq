package org.meteor.client.consumer;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.netty.buffer.ByteBuf;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nonnull;
import org.meteor.client.core.ClientChannel;
import org.meteor.client.core.CombineListener;
import org.meteor.client.core.MessageLedger;
import org.meteor.client.core.MessageRouter;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.common.message.MessageId;
import org.meteor.remote.proto.MessageMetadata;
import org.meteor.remote.proto.client.MessagePushSignal;
import org.meteor.remote.proto.client.NodeOfflineSignal;
import org.meteor.remote.proto.client.TopicChangedSignal;
import org.meteor.remote.util.NetworkUtil;
import org.meteor.remote.util.ProtoBufUtil;

/**
 * DefaultConsumerListener is a final class that implements the CombineListener and MeterBinder interfaces.
 * It acts as an event listener for consumer-related events and provides methods to handle different types of signals
 * and messages for a consumer.
 */
final class DefaultConsumerListener implements CombineListener, MeterBinder {
    /**
     * Logger instance for the DefaultConsumerListener class.
     * Utilizes InternalLogger to provide logging capabilities specific to this class.
     */
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(DefaultConsumerListener.class);
    /**
     * Metric name for tracking the number of pending Netty tasks in the consumer.
     * This constant is used to register and monitor a metric that keeps track of tasks
     * that are pending execution within the Netty event loop for the consumer.
     */
    private static final String METRICS_NETTY_PENDING_TASK_NAME = "consumer_netty_pending_task";
    /**
     * The consumer that handles inbound messages for the DefaultConsumerListener.
     * This consumer is responsible for processing the messages received from clients.
     */
    private final DefaultConsumer consumer;
    /**
     * Array of MessageHandler instances used to handle incoming messages
     * within the DefaultConsumerListener context.
     */
    private final MessageHandler[] handlers;
    /**
     * A mapping of integer markers to their respective {@link MessageHandler} instances.
     * This map is used to retrieve the appropriate handler based on a given marker value
     * within the context of a {@link DefaultConsumerListener}.
     */
    private final IntObjectMap<MessageHandler> markerOfHandlers;
    /**
     * A configuration object for the consumer.
     * <p>
     * This immutable {@code ConsumerConfig} instance holds various settings that are used to configure
     * parameters such as timeouts, retry delays, thread limits, and other consumer-specific settings.
     * <p>
     * It is provided to the {@code DefaultConsumerListener} during its instantiation to customize
     * the behavior and capabilities of the consumer.
     */
    private final ConsumerConfig consumerConfig;
    /**
     * An EventExecutorGroup instance used to manage a group of EventExecutor instances
     * for handling asynchronous tasks related to the DefaultConsumerListener's operation.
     */
    private final EventExecutorGroup group;
    /**
     * A concurrent hash map that holds futures marked as obsolete.
     * The key is a String representing the unique identifier of the future task.
     * The value is a Future<?> object representing the result of an asynchronous computation.
     *
     * This map is used to keep track of futures that are no longer needed,
     * allowing for their eventual cancellation or cleanup.
     */
    private final Map<String, Future<?>> obsoleteFutures = new ConcurrentHashMap<>();

    /**
     * Initializes a new instance of the DefaultConsumerListener class.
     *
     * @param consumer the consumer instance
     * @param consumerConfig the configuration settings for the consumer
     * @param listener the message listener for handling incoming messages
     */
    public DefaultConsumerListener(Consumer consumer, ConsumerConfig consumerConfig, MessageListener listener) {
        this.consumer = (DefaultConsumer) consumer;
        this.consumerConfig = consumerConfig;

        int shardCount = Math.max(consumerConfig.getHandlerShardLimit(), consumerConfig.getHandlerShardLimit());
        this.handlers = new MessageHandler[shardCount];
        int handlerPendingCount = consumerConfig.getHandlerPendingLimit();
        this.group =
                NetworkUtil.newEventExecutorGroup(consumerConfig.getHandlerThreadLimit(), "consumer-message-group");
        for (int i = 0; i < shardCount; i++) {
            Semaphore semaphore = new Semaphore(handlerPendingCount);
            MessageHandler messageHandler = new MessageHandler(String.valueOf(i), semaphore, group.next(), this.consumer.getSubscribeShips(), listener);
            this.handlers[i] = messageHandler;
        }
        this.markerOfHandlers = new IntObjectHashMap<>(shardCount);
    }

    /**
     * Callback method invoked when a channel is closed.
     *
     * @param channel the channel that has been closed
     */
    @Override
    public void onChannelClosed(ClientChannel channel) {
        consumer.touchChangedTask();
    }

    /**
     * Handles the event when the topic of a client channel changes. This method checks if the consumer
     * contains a router for the specified topic and refreshes the router if necessary.
     *
     * @param channel the client channel that triggered the topic change event
     * @param signal the signal containing the information about the topic change
     */
    @Override
    public void onTopicChanged(ClientChannel channel, TopicChangedSignal signal) {
        String topic = signal.getTopic();
        if (!consumer.containsRouter(topic)) {
            if (logger.isDebugEnabled()) {
                logger.debug("The consumer listener doesn't contains topic[{}] message router", topic);
            }
            return;
        }

        int ledgerId = signal.getLedger();
        int ledgerVersion = signal.getLedgerVersion();
        consumer.getExecutor().schedule(() -> {
            try {
                if (consumer.containsRouter(topic)) {
                    MessageRouter router = consumer.getRouter(topic);
                    if (router == null) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("The consumer listener that topic[{}] message router is empty", topic);
                        }
                        return;
                    }

                    MessageLedger ledger = router.ledger(ledgerId);
                    if (ledger == null || ledgerVersion == 0 || ledger.version() < ledgerVersion) {
                        consumer.refreshRouter(topic, channel);
                    }

                    consumer.touchChangedTask();
                }
            } catch (Throwable t) {
                if (logger.isErrorEnabled()) {
                    logger.error("The consumer on-topic-change failure", t);
                }
            }
        }, ThreadLocalRandom.current().nextInt(5000), TimeUnit.MILLISECONDS);
    }

    /**
     * Handles the reception of a push message from a client channel.
     *
     * @param channel the client channel through which the message was pushed
     * @param signal the signal containing metadata about the pushed message
     * @param data the actual message content in ByteBuf format
     */
    @Override
    public void onPushMessage(ClientChannel channel, MessagePushSignal signal, ByteBuf data) {
        int ledger = signal.getLedger();
        int marker = signal.getMarker();
        int epoch = signal.getEpoch();
        long index = signal.getIndex();
        if (!channel.equals(consumer.getReadyChannels().get(ledger))) {
            try {
                MessageMetadata metadata = ProtoBufUtil.readProto(data, MessageMetadata.parser());
                String topic = metadata.getTopic();
                obsoleteFutures.computeIfAbsent(ledger + "@" + channel.id(),
                        k -> consumer.getExecutor().schedule(() -> {
                            if (channel.isActive() && !channel.equals(consumer.getReadyChannels().get(ledger))) {
                                consumer.doCleanSubscribe(channel, topic, ledger);
                            }

                            obsoleteFutures.remove(k);
                        }, consumerConfig.getControlRetryDelayMilliseconds(), TimeUnit.MILLISECONDS));
            } catch (Throwable t) {
                if (logger.isErrorEnabled()) {
                    logger.error("Meteor on push-message failure", t);
                }
            }
            return;
        }

        AtomicReference<MessageId> sequence = consumer.getLedgerSequences().get(ledger);
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
            MessageHandler handler = getHandler(marker, handlers.length);
            handler.handle(channel, marker, id, data);
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("The consumer listener that handle message failure", t);
            }
        }
    }

    /**
     * Retrieves the MessageHandler associated with the given marker. If no handler exists for the marker,
     * a new handler is created based on the consistent hash of the marker and length and stored in the map.
     *
     * @param marker the unique identifier for the handler.
     * @param length the length used for consistent hashing to determine the handler index.
     * @return the MessageHandler associated with the specified marker.
     */
    private MessageHandler getHandler(int marker, int length) {
        return markerOfHandlers.computeIfAbsent(marker, v -> handlers[consistentHash(marker, length)]);
    }

    /**
     * Computes a consistent hash value for the given input and bucket count.
     *
     * @param input the input value to be hashed
     * @param buckets the number of buckets for the hash
     * @return the computed hash value within the range of [0, buckets)
     */
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

    /**
     * Handles the event when a node goes offline.
     *
     * @param channel the client channel through which the node was connected
     * @param signal  the signal containing information about the node offline event
     */
    @Override
    public void onNodeOffline(ClientChannel channel, NodeOfflineSignal signal) {
        CombineListener.super.onNodeOffline(channel, signal);
    }

    /**
     * This method is called when the listener has completed its tasks, allowing for any necessary
     * cleanup and resource deallocation. Specifically, it ensures that the event loop group,
     * if present, is shut down gracefully. It then iterates through all event executors
     * within the group, ensuring each one is terminated properly.
     *
     * @throws InterruptedException If the current thread is interrupted while waiting for
     *                              the termination of event executors.
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Override
    public void listenerCompleted() throws InterruptedException {
        if (group != null) {
            group.shutdownGracefully().sync();
            Iterator<EventExecutor> iterator = group.iterator();
            while (iterator.hasNext()) {
                try {
                    EventExecutor next = iterator.next();
                    while (!next.isTerminated()) {
                        next.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
                    }
                } catch (Exception e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /**
     * Binds the metrics of the consumer to the provided MeterRegistry.
     *
     * @param meterRegistry the registry to which the metrics should be bound
     */
    @Override
    public void bindTo(@Nonnull MeterRegistry meterRegistry) {
        SingleThreadEventExecutor singleThreadEventExecutor = (SingleThreadEventExecutor) consumer.getExecutor();
        registerMetrics("consumer-task", meterRegistry, singleThreadEventExecutor);
        for (EventExecutor eventExecutor : group) {
            try {
                SingleThreadEventExecutor executor = (SingleThreadEventExecutor) eventExecutor;
                registerMetrics("consumer-handler", meterRegistry, executor);
            } catch (Throwable ignored) {
            }
        }
    }

    /**
     * Registers metrics for a given single-thread event executor.
     *
     * @param type                   the type of the metrics to be registered
     * @param meterRegistry          the registry where the metrics will be registered
     * @param singleThreadEventExecutor the single-thread event executor whose metrics are to be monitored
     */
    private void registerMetrics(String type, MeterRegistry meterRegistry, SingleThreadEventExecutor singleThreadEventExecutor) {
        Gauge.builder(METRICS_NETTY_PENDING_TASK_NAME, singleThreadEventExecutor, SingleThreadEventExecutor::pendingTasks)
                .tag("type", type)
                .tag("name", consumer.getName())
                .tag("id", singleThreadEventExecutor.threadProperties().name())
                .register(meterRegistry);
    }
}
