package org.meteor.proxy.support;

public class MeterProxyException extends RuntimeException {
    public MeterProxyException() {
    }

    public MeterProxyException(String message) {
        super(message);
    }

    public MeterProxyException(String message, Throwable cause) {
        super(message, cause);
    }

    public MeterProxyException(Throwable cause) {
        super(cause);
    }

    public MeterProxyException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
