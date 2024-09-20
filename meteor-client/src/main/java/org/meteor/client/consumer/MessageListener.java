package org.meteor.client.consumer;

import io.netty.buffer.ByteBuf;
import org.meteor.common.message.MessageId;

import java.util.Map;

/**
 * Represents a listener that handles incoming messages from a specified topic and queue.
 * This functional interface provides a single method to process messages with associated metadata.
 */
@FunctionalInterface
public interface MessageListener {
    /**
     * Handles the incoming message from the specified topic and queue along with its associated metadata.
     *
     * @param topic     the topic from which the message is received
     * @param queue     the queue from which the message is received
     * @param messageId the unique identifier of the message
     * @param message   the data buffer containing the message payload
     * @param extras    additional metadata associated with the message
     */
    void onMessage(String topic, String queue, MessageId messageId, ByteBuf message, Map<String, String> extras);
}
