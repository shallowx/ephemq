package org.meteor.client.producer;

import io.netty.buffer.ByteBuf;
import java.util.Map;
import org.meteor.common.message.MessageId;

/**
 * The Producer interface defines methods for starting the producer, sending messages
 * synchronously and asynchronously to a specified topic and queue, sending messages in
 * a one-way fashion, and closing the producer.
 */
public interface Producer {
    /**
     * Initializes and starts the producer. This method must be called before any message
     * sending operations can be performed. Typically, this sets up the necessary resources
     * or connections required by the producer.
     */
    void start();

    /**
     * Sends a message to the specified topic and queue with the provided extra properties.
     *
     * @param topic   The topic to which the message should be sent.
     * @param queue   The queue within the topic where the message will be placed.
     * @param message The message payload to be sent as a ByteBuf.
     * @param extras  A map of additional properties to accompany the message.
     * @return The MessageId of the sent message, indicating its unique identifier within the system.
     */
    MessageId send(String topic, String queue, ByteBuf message, Map<String, String> extras);

    /**
     * Send a message to a specified topic and queue with optional extra parameters and a timeout.
     *
     * @param topic the topic to send the message to
     * @param queue the queue within the topic to send the message to
     * @param message the message to be sent, encapsulated in a ByteBuf
     * @param extras additional key-value pairs to send along with the message
     * @param timeout the maximum amount of time to wait for the send operation to complete
     * @return the MessageId representing the unique identifier of the sent message
     */
    MessageId send(String topic, String queue, ByteBuf message, Map<String, String> extras, long timeout);

    /**
     * Sends a message asynchronously to the specified topic and queue.
     *
     * @param topic    the topic to send the message to
     * @param queue    the queue to send the message to
     * @param message  the message payload as a ByteBuf
     * @param extras   additional properties to be sent along with the message
     * @param callback the callback to handle the result of the send operation
     */
    void sendAsync(String topic, String queue, ByteBuf message, Map<String, String> extras, SendCallback callback);

    /**
     * Sends a one-way message to the specified topic and queue.
     *
     * This method does not wait for any acknowledgment from the receiver and completes immediately after the message is sent.
     *
     * @param topic the topic to which the message is to be sent
     * @param queue the queue within the topic to which the message is to be sent
     * @param message the message payload to be sent
     * @param extras additional metadata or properties associated with the message
     */
    void sendOneway(String topic, String queue, ByteBuf message, Map<String, String> extras);

    /**
     * Closes the producer, releasing any resources allocated for message production.
     * This method ensures that all pending messages are sent before shutting down.
     */
    void close();
}
