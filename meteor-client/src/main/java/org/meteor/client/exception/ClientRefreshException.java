package org.meteor.client.exception;

import java.io.Serial;

/**
 * ClientRefreshException is a custom RuntimeException that signals an issue during
 * the client's metadata refresh process.
 */
public class ClientRefreshException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 5391285827332471674L;

    /**
     *
     */
    public ClientRefreshException(String msg) {
        this(msg, null);
    }

    /**
     * Constructs a new ClientRefreshException with the specified detail message and cause.
     *
     * @param msg   the detail message, which can be retrieved by the Throwable.getMessage() method.
     * @param cause the cause, which can be retrieved by the Throwable.getCause() method. A null value indicates that the cause is nonexistent or unknown.
     */
    public ClientRefreshException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
