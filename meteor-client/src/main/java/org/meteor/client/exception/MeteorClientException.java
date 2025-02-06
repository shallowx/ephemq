package org.meteor.client.exception;

import java.io.Serial;

/**
 * A custom exception class that extends {@link RuntimeException} to represent
 * error scenarios specific to the Meteor client operations.
 * <p>
 * This exception can be thrown to indicate various failure conditions
 * encountered while interacting with a Meteor client. It supports
 * an optional cause to be passed along with the exception message.
 */
public class MeteorClientException extends RuntimeException {
    /**
     * Defines a unique identifier for this Serializable class to verify that
     * the sender and receiver of a serialized object maintain serialization
     * compatibility.
     * <p>
     * This identifier is used during the deserialization process to ensure
     * that a class definition is compatible with the serialized object.
     */
    @Serial
    private static final long serialVersionUID = 5391285827332471674L;

    /**
     * Constructs a new MeteorClientException with the specified detail message.
     *
     * @param msg the detail message to be provided for the exception.
     */
    public MeteorClientException(String msg) {
        this(msg, null);
    }

    /**
     * Constructs a new MeteorClientException with the specified detail message
     * and cause.
     *
     * @param msg the detail message explaining the reason for the exception
     * @param cause the underlying cause of the exception (can be null)
     */
    public MeteorClientException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
