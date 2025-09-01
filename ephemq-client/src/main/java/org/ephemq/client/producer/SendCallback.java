package org.ephemq.client.producer;

import org.ephemq.common.message.MessageId;

/**
 * The SendCallback interface represents a callback to be used for asynchronous message send operations.
 * It is a functional interface that contains a single method, {@code onCompleted}, which is called when
 * the send operation is completed.
 * <p>
 * Implementors of this interface can define custom behavior for handling the completion of the message send.
 * This can include logic for logging, re-attempting the send, handling errors, or updating application state
 * based on the result of the send operation.
 */
@FunctionalInterface
public interface SendCallback {
    /**
     * Called when the asynchronous send operation is completed. This method will be
     * invoked after the send operation finishes, whether it completes successfully
     * or encounters an error.
     *
     * @param messageId the unique identifier of the sent message if the operation
     *                  was successful, otherwise null
     * @param t         the throwable encountered if the send operation failed,
     *                  otherwise null
     */
    void onCompleted(MessageId messageId, Throwable t);
}
