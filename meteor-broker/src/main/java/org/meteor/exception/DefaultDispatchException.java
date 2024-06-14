package org.meteor.exception;

import java.io.Serial;

public class DefaultDispatchException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 5391285827332471674L;

    public DefaultDispatchException(String msg) {
        this(msg, null);
    }

    public DefaultDispatchException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
