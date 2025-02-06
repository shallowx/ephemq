package org.meteor.remote.exception;

import java.io.Serial;

/**
 * Exception thrown to indicate an error occurred during the encoding process
 * in a remoting communication context.
 * <p>
 * This exception typically signals that a message's content length exceeds the
 * permissible limit, as defined by the specific remoting protocol being used.
 */
public class RemotingEncoderException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 6926716840699621852L;

    /**
     * Constructs a new {@code RemotingEncoderException} with no detail message or cause.
     * <p>
     * This constructor is useful for creating a default instance of the exception,
     * which may be subsequently initialized with a more specific message or cause.
     */
    public RemotingEncoderException() {
    }

    /**
     * Constructs a new RemotingEncoderException with the specified detail message and cause.
     *
     * @param message the detail message (which is saved for later retrieval by the {@link Throwable#getMessage()} method)
     * @param cause the cause (which is saved for later retrieval by the {@link Throwable#getCause()} method).
     */
    public RemotingEncoderException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a new RemotingEncoderException with the specified detail message.
     *
     * @param message the detail message explaining the reason for the exception
     */
    public RemotingEncoderException(String message) {
        super(message);
    }

    /**
     * Constructs a new {@code RemotingEncoderException} with the specified cause.
     *
     * @param cause the cause of this exception, which may be retrieved later by the
     *              {@link Throwable#getCause()} method (a <tt>null</tt> value is permitted,
     *              and indicates that the cause is nonexistent or unknown)
     */
    public RemotingEncoderException(Throwable cause) {
        super(cause);
    }
}
