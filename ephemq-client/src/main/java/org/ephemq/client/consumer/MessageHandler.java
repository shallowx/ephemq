package org.ephemq.client.consumer;

import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.EventExecutor;
import org.ephemq.client.core.ClientChannel;
import org.ephemq.common.logging.InternalLogger;
import org.ephemq.common.logging.InternalLoggerFactory;
import org.ephemq.common.message.MessageId;
import org.ephemq.remote.proto.MessageMetadata;
import org.ephemq.remote.util.ProtoBufUtil;

import java.util.Map;
import java.util.concurrent.Semaphore;

/**
 * MessageHandler is responsible for handling messages received from a client channel.
 * It manages task execution, concurrency control, and message processing.
 */
final class MessageHandler {
    /**
     * Logger instance used for logging events and errors in the MessageHandler class.
     * This logger is instantiated using the InternalLoggerFactory to associate
     * logging with the MessageHandler class context.
     */
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MessageHandler.class);
    /**
     * The unique identifier for this instance of MessageHandler.
     * This ID can be used for logging, tracking, or other identification purposes.
     */
    private final String id;
    /**
     * A semaphore to control the concurrency of message handling.
     * This semaphore ensures that the tasks execute in a controlled
     * manner, preventing excessive simultaneous access which could
     * lead to resource contention or other concurrency issues.
     */
    private final Semaphore semaphore;
    /**
     * The executor used to manage the execution of tasks within the MessageHandler.
     * It ensures that tasks are run in an asynchronous manner, providing concurrency control
     * and preventing blocking operations on the main thread.
     */
    private final EventExecutor executor;
    /**
     * A map that holds subscription information.
     * The outer map keys are queue names and the values are maps that hold topic names and their corresponding modes.
     * <p>
     * The structure is as follows:
     * - Key (String): queue name
     * - Value (Map<String, Mode>): a map where the key is the topic name and the value is its subscription mode.
     * <p>
     * This structure helps in determining the subscription mode for each topic in different queues.
     */
    private final Map<String, Map<String, Mode>> subscribeShips;
    /**
     * The listener for message events.
     * <p>
     * This listener will be invoked to handle specific messages
     * received from a client channel, passing details such as
     * the topic, queue, messageId, message data, and any additional
     * metadata.
     * <p>
     * The listener's implementation of the onMessage method will
     * determine the actions taken for each message.
     */
    private final MessageListener listener;

    /**
     * Constructs a new MessageHandler.
     *
     * @param id the unique identifier for the MessageHandler instance
     * @param semaphore the semaphore used for managing concurrency control
     * @param executor the executor responsible for handling tasks
     * @param subscribeShips the mapping of subscribed topics and queues with their respective modes
     * @param listener the listener that will handle messages received from the client channel
     */
    public MessageHandler(String id, Semaphore semaphore, EventExecutor executor, Map<String,
            Map<String, Mode>> subscribeShips, MessageListener listener) {
        this.id = id;
        this.semaphore = semaphore;
        this.executor = executor;
        this.subscribeShips = subscribeShips;
        this.listener = listener;
    }

    /**
     * Handles the incoming message from the client channel.
     * It ensures the executor is not shutting down, acquires the semaphore,
     * and processes the message data in a separate thread.
     *
     * @param channel the client channel from which the message is received
     * @param marker an integer marker indicating the type or status of the message
     * @param messageId the unique identifier of the message
     * @param data the data buffer containing the message contents
     */
    void handle(ClientChannel channel, int marker, MessageId messageId, ByteBuf data) {
        if (executor.isShuttingDown()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Message handler[{}] executor is shutting down", id);
            }
            return;
        }
        semaphore.acquireUninterruptibly();
        data.retain();
        try {
            executor.execute(() -> doHandle(channel, marker, messageId, data));
        } catch (Throwable t) {
            data.release();
            semaphore.release();
            if (logger.isErrorEnabled()) {
                logger.error("Consumer handle[{}] message failed", id, t);
            }
        }
    }

    /**
     * Handles the message received from the client channel.
     *
     * @param channel  the client channel from which the message is received
     * @param marker   an integer marker value associated with the message
     * @param messageId the unique identifier of the message
     * @param data     the data buffer containing the message payload
     */
    private void doHandle(ClientChannel channel, int marker, MessageId messageId, ByteBuf data) {
        int length = data.readableBytes();
        try {
            MessageMetadata metadata = ProtoBufUtil.readProto(data, MessageMetadata.parser());
            String topic = metadata.getTopic();
            String queue = metadata.getQueue();
            Map<String, Mode> topicModes = subscribeShips.get(queue);
            Mode mode = topicModes == null ? null : topicModes.get(topic);
            if (mode != null && mode != Mode.DELETE) {
                listener.onMessage(topic, queue, messageId, data, metadata.getExtrasMap());
            }
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error(" Consumer handle[{}] message failed, address[{}] marker[{}] messageId[{}] length[{}]", id, channel.address(), marker, messageId, length, t);
            }
        } finally {
            data.release();
            semaphore.release();
        }
    }
}
