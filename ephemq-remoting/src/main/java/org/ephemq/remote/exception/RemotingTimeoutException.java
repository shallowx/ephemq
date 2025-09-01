package org.ephemq.remote.exception;

import java.io.Serial;

/**
 * Exception thrown when a remoting operation times out.
 * <p>
 * This exception is used to indicate that a response was not received
 * within the expected time frame for a remote communication.
 */
public class RemotingTimeoutException extends Exception {

    @Serial
    private static final long serialVersionUID = 4106899185095245979L;

    /**
     * Constructs a new RemotingTimeoutException with the specified detail message.
     *
     * @param message the detail message
     */
    public RemotingTimeoutException(String message) {
        super(message);
    }

    /**
     * Constructs a new RemotingTimeoutException with the specified address and timeout duration.
     *
     * @param addr the address of the remote service that was being communicated with when the timeout occurred
     * @param timeoutMillis the duration in milliseconds that was waited before timing out
     */
    public RemotingTimeoutException(String addr, long timeoutMillis) {
        this(addr, timeoutMillis, null);
    }

    /**
     * Constructs a new RemotingTimeoutException with the specified remote address,
     * timeout period, and cause.
     *
     * @param addr the address of the remote server.
     * @param timeoutMillis the timeout duration in milliseconds.
     * @param cause the underlying cause of the exception.
     */
    public RemotingTimeoutException(String addr, long timeoutMillis, Throwable cause) {
        super(String.format("Wait response on the channel <%s> timeout, %s(ms)", addr, timeoutMillis), cause);
    }
}
