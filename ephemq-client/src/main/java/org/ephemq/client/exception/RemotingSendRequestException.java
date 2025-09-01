package org.ephemq.client.exception;

import java.io.Serial;

/**
 * RemotingSendRequestException is a custom unchecked exception that
 * indicates a failure while sending a remoting request.
 * This exception signals that something went wrong during the process
 * of dispatching a message to a remote address.
 */
public class RemotingSendRequestException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 5391285827332471674L;

    /**
     * Constructs a new RemotingSendRequestException with a specified remote address.
     *
     * @param addr the remote address associated with the request failure.
     */
    public RemotingSendRequestException(String addr) {
        this(addr, null);
    }

    /**
     * Constructs a RemotingSendRequestException with the specified detail message and cause.
     *
     * @param msg   the detail message, saved for later retrieval by the {@link Throwable#getMessage()} method.
     * @param cause the cause, saved for later retrieval by the {@link Throwable#getCause()} method.
     *              A null value is permitted and indicates that the cause is nonexistent or unknown.
     */
    public RemotingSendRequestException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
