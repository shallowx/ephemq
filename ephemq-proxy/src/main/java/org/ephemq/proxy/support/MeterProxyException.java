package org.ephemq.proxy.support;

/**
 * Represents an exception specifically thrown by the Meter Proxy.
 * This exception extends RuntimeException and can be used to indicate
 * various issues encountered within the Meter Proxy operations.
 */
public class MeterProxyException extends RuntimeException {
    /**
     * Constructs a new MeterProxyException with {@code null} as its detail message.
     * The cause is not initialized, and may subsequently be initialized by a call to {@link Throwable#initCause}.
     */
    public MeterProxyException() {
    }

    /**
     * Constructs a new MeterProxyException with the specified detail message.
     *
     * @param message the detail message. This message is saved for later retrieval
     *                by the Throwable.getMessage() method.
     */
    public MeterProxyException(String message) {
        super(message);
    }

    /**
     * Constructs a new MeterProxyException with the specified detail message and cause.
     *
     * @param message the detail message, saved for later retrieval by the {@link Throwable#getMessage()} method.
     * @param cause   the cause, saved for later retrieval by the {@link Throwable#getCause()} method.
     *                A null value is permitted, and indicates that the cause is nonexistent or unknown.
     */
    public MeterProxyException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a new MeterProxyException with the specified cause.
     *
     * @param cause the cause of the exception (which is saved for later retrieval
     *              by the {@link Throwable#getCause()} method). A {@code null} value
     *              is permitted, and indicates that the cause is nonexistent or unknown.
     */
    public MeterProxyException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a new MeterProxyException with the specified detail message, cause,
     * suppression enabled or disabled, and writable stack trace enabled or disabled.
     *
     * @param message            the detail message.
     * @param cause              the cause (which is saved for later retrieval by the {@link Throwable#getCause()} method).
     * @param enableSuppression  whether or not suppression is enabled or disabled.
     * @param writableStackTrace whether or not the stack trace should be writable.
     */
    public MeterProxyException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
