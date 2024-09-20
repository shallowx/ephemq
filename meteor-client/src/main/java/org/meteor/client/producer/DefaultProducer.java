package org.meteor.client.producer;

import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.meteor.client.core.Client;
import org.meteor.client.core.ClientChannel;
import org.meteor.client.core.CombineListener;
import org.meteor.client.core.MessageLedger;
import org.meteor.client.core.MessageRouter;
import org.meteor.client.exception.RemotingSendRequestException;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.common.message.MessageId;
import org.meteor.common.util.TopicPatternUtil;
import org.meteor.remote.proto.MessageMetadata;
import org.meteor.remote.proto.server.SendMessageRequest;
import org.meteor.remote.proto.server.SendMessageResponse;
import org.meteor.remote.util.ByteBufUtil;

/**
 * DefaultProducer is an implementation of the Producer interface. It provides methods
 * to send messages synchronously, asynchronously, and in a one-way fashion to a specified
 * topic and queue on a messaging system.
 * <p>
 * The class manages the lifecycle of the producer, including starting, closing, and
 * ensuring the appropriate state transitions. It uses a client internally to handle
 * the actual message sending.
 */
public class DefaultProducer implements Producer {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(DefaultProducer.class);
    /**
     * The name associated with this DefaultProducer instance.
     * This is a unique identifier used to distinguish between
     * different producer instances within the system.
     */
    private final String name;
    /**
     * Configuration settings for the Producer.
     *
     * This variable holds the configuration details necessary for the Producer to
     * function correctly. It includes settings for timeouts, compression, and other
     * client-specific configurations.
     */
    private final ProducerConfig config;
    /**
     * The Client instance used for interacting with remote servers and handling
     * communication-related tasks within the DefaultProducer class.
     */
    private final Client client;
    /**
     * A map that holds ready client channels, keyed by an integer ID.
     * This map is thread-safe and is used to store channels that are
     * ready to send or receive data.
     */
    private final Map<Integer, ClientChannel> readyChannels = new ConcurrentHashMap<>();
    /**
     * The CombineListener instance used by DefaultProducer to handle various events
     * such as channel activations, closures, message pushes, topic changes, node
     * offline events, and synchronization messages. This listener is responsible
     * for processing and reacting to these events, allowing for custom behavior
     * to be defined by the implementor.
     */
    private final CombineListener listener;
    /**
     * Represents the state of the DefaultProducer.
     * This variable indicates whether the producer is currently active or inactive.
     * It is marked as volatile to ensure visibility of changes across threads.
     */
    private volatile boolean state = false;

    /**
     * Constructs a DefaultProducer instance with a specified name and configuration.
     *
     * @param name the name of the producer
     * @param config the configuration settings for the producer
     */
    public DefaultProducer(String name, ProducerConfig config) {
        this(name, config, null);
    }

    /**
     * Constructs a new DefaultProducer instance.
     *
     * @param name the name of the producer
     * @param config the configuration settings for the producer
     * @param clientListener the listener for client events, or null to use a default listener
     * @throws NullPointerException if the config is null
     */
    public DefaultProducer(String name, ProducerConfig config, CombineListener clientListener) {
        this.name = name;
        this.config = Objects.requireNonNull(config, "Producer config not found");
        this.listener = clientListener == null ? new DefaultProducerListener(this) : clientListener;
        this.client = new Client(name, config.getClientConfig(), listener);
    }

    /**
     * Starts the producer.
     *
     * This method first checks if the producer is already running. If it is, a warning message
     * is logged, and the method returns early to avoid restarting the producer. If the producer
     * is not running, it sets the state to running and starts the client.
     */
    @Override
    public void start() {
        if (isRunning()) {
            if (logger.isWarnEnabled()) {
                logger.warn("Producer[{}] is running, don't run it replay", name);
            }
            return;
        }
        state = true;
        client.start();
    }

    /**
     * Checks if the producer is currently running.
     *
     * @return true if the producer is running, false otherwise.
     */
    private boolean isRunning() {
        return state;
    }

    /**
     * Send a message to a specified topic and queue with optional extra parameters.
     *
     * @param topic the topic to send the message to
     * @param queue the queue within the topic to send the message to
     * @param message the message to be sent, encapsulated in a ByteBuf
     * @param extras additional key-value pairs to send along with the message
     * @return the MessageId representing the unique identifier of the sent message
     */
    @Override
    public MessageId send(String topic, String queue, ByteBuf message, Map<String, String> extras) {
        return send(topic, queue, message, extras, -1);
    }

    /**
     * Sends a message to the specified topic and queue with extra headers and a specified timeout.
     *
     * @param topic the topic to which the message will be sent
     * @param queue the queue within the topic for the message
     * @param message the message payload to be sent
     * @param extras additional key-value pairs to include as headers
     * @param timeout the time in milliseconds to wait for the send operation to complete
     * @return the unique identifier of the sent message
     * @throws RemotingSendRequestException if the message fails to send
     */
    @Override
    public MessageId send(String topic, String queue, ByteBuf message, Map<String, String> extras, long timeout) {
        try {
            Promise<SendMessageResponse> promise = ImmediateEventExecutor.INSTANCE.newPromise();
            doSend(topic, queue, message, extras, config.getSendTimeoutMilliseconds(), promise);
            SendMessageResponse response = timeout > 0 ? promise.get(Math.max(timeout, 3_000L), TimeUnit.MILLISECONDS) : promise.get();
            return new MessageId(response.getLedger(), response.getEpoch(), response.getIndex());
        } catch (Throwable t) {
            throw new RemotingSendRequestException(
                    STR."Message send failed, topic[\{topic}] queue[\{queue}] length[\{ByteBufUtil.bufLength(
                            message)}]", t);
        } finally {
            ByteBufUtil.release(message);
        }
    }

    /**
     * Sends a message asynchronously to the specified topic and queue.
     *
     * @param topic The topic to which the message should be sent.
     * @param queue The queue to which the message should be sent.
     * @param message The ByteBuf containing the message content to be sent.
     * @param extras A map of additional metadata to be included with the message.
     * @param callback The callback to be invoked upon the completion of the send operation. Can be null for one-way sends.
     * @throws RemotingSendRequestException if the send request fails.
     */
    @Override
    public void sendAsync(String topic, String queue, ByteBuf message, Map<String, String> extras, SendCallback callback) {
        try {
            if (callback == null) {
                doSend(topic, queue, message, extras, config.getSendOnewayTimeoutMilliseconds(), null);
                return;
            }

            Promise<SendMessageResponse> promise = ImmediateEventExecutor.INSTANCE.newPromise();
            promise.addListener((GenericFutureListener<Future<SendMessageResponse>>) f -> {
                if (f.isSuccess()) {
                    SendMessageResponse res = f.getNow();
                    callback.onCompleted(new MessageId(res.getLedger(), res.getEpoch(), res.getIndex()), null);
                } else {
                    callback.onCompleted(null, f.cause());
                }
            });
            doSend(topic, queue, message, extras, config.getSendAsyncTimeoutMilliseconds(), promise);
        } catch (Throwable t) {
            throw new RemotingSendRequestException(
                    STR."Message send failed, topic[\{topic}] queue[\{queue}] length[\{ByteBufUtil.bufLength(
                            message)}]", t);
        } finally {
            ByteBufUtil.release(message);
        }
    }

    /**
     * Sends a message to the specified topic and queue in a one-way fashion.
     * This means that no acknowledgment or response is expected from the receiver.
     *
     * @param topic the topic to which the message should be sent.
     * @param queue the queue within the topic to which the message should be sent.
     * @param message the message payload to be sent.
     * @param extras additional metadata or properties to be included with the message.
     */
    @Override
    public void sendOneway(String topic, String queue, ByteBuf message, Map<String, String> extras) {
        try {
            doSend(topic, queue, message, extras, config.getSendAsyncTimeoutMilliseconds(), null);
        } catch (Throwable t) {
            throw new RemotingSendRequestException(
                    STR."Message send oneway failed, topic[\{topic}] queue[\{queue}] length[\{ByteBufUtil.bufLength(
                            message)}]", t);
        } finally {
            ByteBufUtil.release(message);
        }
    }

    /**
     * Sends a message to a specified topic and queue with a timeout and promise for the response.
     *
     * @param topic the topic to which the message will be sent
     * @param queue the specific queue within the topic
     * @param message the message to be sent, represented by a ByteBuf object
     * @param extras additional metadata as a map of key-value pairs
     * @param timeoutMs the timeout in milliseconds for the send operation
     * @param promise a promise representing the response of the send operation
     */
    private void doSend(String topic, String queue, ByteBuf message, Map<String, String> extras, int timeoutMs, Promise<SendMessageResponse> promise) {
        TopicPatternUtil.validateQueue(queue);
        TopicPatternUtil.validateTopic(topic);

        MessageRouter router = client.fetchRouter(topic);
        if (router == null) {
            throw new IllegalStateException(STR."Message router[topic:\{topic}] not found");
        }

        MessageLedger ledger = router.routeLedger(queue);
        if (ledger == null) {
            throw new IllegalStateException(STR."Message ledger[queue:\{queue}] not found");
        }

        SocketAddress leader = ledger.leader();
        if (leader == null) {
            throw new IllegalStateException(STR."Message leader[topic:\{topic},queue:\{queue}] not found");
        }

        int marker = router.routeMarker(queue);
        SendMessageRequest request = SendMessageRequest.newBuilder().setLedger(ledger.id()).setMarker(marker).build();
        MessageMetadata metadata = buildMetadata(topic, queue, extras);
        ClientChannel channel = getReadyChannel(leader, ledger.id());
        channel.invoker().sendMessage(timeoutMs, promise, request, metadata, message);
    }

    /**
     * Retrieves an active and ready ClientChannel associated with the given ledger. If the channel is not
     * found in the cache, it attempts to fetch an active channel from the client and updates the cache.
     *
     * @param address the SocketAddress to check or use when fetching a new ClientChannel. Can be null.
     * @param ledger the ledger identifier for which the channel is to be retrieved.
     * @return the active and ready ClientChannel associated with the given ledger and address.
     * @throws IllegalStateException if the address is null and the channel associated with the ledger is not found.
     */
    private ClientChannel getReadyChannel(SocketAddress address, int ledger) {
        ClientChannel channel = readyChannels.get(ledger);
        if (channel != null && channel.isActive() && (address == null || channel.address().equals(address))) {
            return channel;
        }

        if (address == null) {
            throw new IllegalStateException(STR."Channel address not found, ledger[\{ledger}]");
        }

        synchronized (readyChannels) {
            channel = readyChannels.get(ledger);
            if (channel != null && channel.isActive() && channel.address().equals(address)) {
                return channel;
            }
            channel = client.getActiveChannel(address);
            readyChannels.put(ledger, channel);
            return channel;
        }
    }

    /**
     * Builds and returns a MessageMetadata object using the provided topic, queue, and extras.
     *
     * @param topic the topic to set in the message metadata
     * @param queue the queue to set in the message metadata
     * @param extras additional key-value pairs to include in the message metadata
     * @return the constructed MessageMetadata object
     */
    private MessageMetadata buildMetadata(String topic, String queue, Map<String, String> extras) {
        MessageMetadata.Builder builder = MessageMetadata.newBuilder().setTopic(topic).setQueue(queue);
        if (extras != null && !extras.isEmpty()) {
            for (Map.Entry<String, String> entry : extras.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                if (key != null && value != null) {
                    builder.putExtras(key, value);
                }
            }
        }
        return builder.build();
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
     * Fetches a MessageRouter instance associated with the given topic.
     *
     * @param topic the topic for which the message router needs to be fetched
     * @return the MessageRouter associated with the specified topic
     */
    MessageRouter fetchRouter(String topic) {
        return client.fetchRouter(topic);
    }

    /**
     * Refreshes the router associated with a specific topic using the provided client channel.
     *
     * @param topic   the topic for which the router needs to be refreshed.
     * @param channel the client channel used to perform the router refresh operation.
     */
    void refreshRouter(String topic, ClientChannel channel) {
        client.refreshRouter(topic, channel);
    }

    /**
     * Retrieves the name of the producer.
     *
     * @return the name of the producer
     */
    String getName() {
        return name;
    }

    /**
     * Closes the producer and releases associated resources.
     *
     * This method performs several operations to gracefully shut down the producer:
     *
     * 1. Checks if the producer is currently running.
     *    - If not running, logs a warning message and exits.
     * 2. Sets the producer's internal state to inactive.
     * 3. Notifies the listener about the completion of tasks.
     *    - Any interruptions during this notification are ignored.
     * 4. Closes the underlying client connection.
     */
    @Override
    public void close() {
        if (!isRunning()) {
            if (logger.isWarnEnabled()) {
                logger.warn("Producer[{}] was closed, don't execute it replay", name);
            }
            return;
        }

        state = false;
        try {
            listener.listenerCompleted();
        } catch (InterruptedException ignored) {
            // keep empty
        }
        client.close();
    }
}
