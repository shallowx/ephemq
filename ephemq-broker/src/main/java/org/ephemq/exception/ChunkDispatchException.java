package org.ephemq.exception;

import java.io.Serial;

/**
 * This exception is thrown to indicate an error that occurs during the dispatching of chunks.
 * It is a specific type of RuntimeException that provides additional context with the error message
 * and an optional cause.
 */
public class ChunkDispatchException extends RuntimeException {
    /**
     * Serial version UID for ensuring compatibility during the deserialization process.
     * This unique identifier verifies that the sender and receiver of a serialized
     * object maintain binary compatibility.
     */
    @Serial
    private static final long serialVersionUID = 5391285827332471674L;

    /**
     * Constructs a new ChunkDispatchException with the specified detail message.
     *
     * @param msg the detail message provided for this exception
     */
    public ChunkDispatchException(String msg) {
        this(msg, null);
    }

    /**
     * Constructs a new ChunkDispatchException with the specified detail message and cause.
     *
     * @param msg   the detail message, which provides additional information about the error.
     * @param cause the cause of the error, which allows for chaining exceptions to represent
     *              error conditions that caused this exception.
     */
    public ChunkDispatchException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
