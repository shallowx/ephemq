package org.meteor.exception;

import java.io.Serial;

public class DispatchException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 5391285827332471674L;

    public DispatchException(String msg) {
        this(msg, null);
    }

    public DispatchException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
