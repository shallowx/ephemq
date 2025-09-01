package org.ephemq.exception;

import java.io.Serial;

/**
 * Represents exceptions that occur while handling topics.
 * This class provides constructors to create exceptions with specific messages and underlying causes.
 */
public class TopicHandleException extends RuntimeException {
    /**
     * Unique identifier for the Serializable class to ensure serialization compatibility.
     * Used to verify that the sender and receiver of a serialized object have loaded classes
     * for that object that are compatible with respect to serialization.
     */
    @Serial
    private static final long serialVersionUID = 5391285827332471674L;

    /**
     * Constructs a new TopicHandleException with the specified detail message.
     *
     * @param msg the detail message, which can be retrieved later by the getMessage() method.
     */
    public TopicHandleException(String msg) {
        this(msg, null);
    }

    /**
     * Constructs a new TopicHandleException with the specified detail message and cause.
     *
     * @param msg   the detail message. The detail message is saved for later retrieval by the getMessage() method.
     * @param cause the cause (which is saved for later retrieval by the getCause() method). A null value is permitted,
     *              and indicates that the cause is nonexistent or unknown.
     */
    public TopicHandleException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
