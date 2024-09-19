package org.meteor.exception;

import java.io.Serial;

/**
 * DefaultDispatchException is a custom unchecked exception that extends RuntimeException.
 * It is used to signal errors related to the default dispatch mechanism in the application.
 */
public class DefaultDispatchException extends RuntimeException {
    /**
     * The serialVersionUID is a unique identifier for the {@code DefaultDispatchException} class
     * used during deserialization to verify that the sender and receiver of a serialized object
     * have loaded classes for that object that are compatible with respect to serialization.
     */
    @Serial
    private static final long serialVersionUID = 5391285827332471674L;

    /**
     * Constructs a DefaultDispatchException with a specified detail message.
     *
     * @param msg The detail message providing information about the exception.
     */
    public DefaultDispatchException(String msg) {
        this(msg, null);
    }

    /**
     * Constructs a new DefaultDispatchException with the specified detail message and cause.
     *
     * @param msg   the detail message, saved for later retrieval by the getMessage() method.
     * @param cause the cause, saved for later retrieval by the getCause() method. A null value is permitted and indicates that the cause is nonexistent or unknown.
     */
    public DefaultDispatchException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
