package org.meteor.client.exception;

import java.io.Serial;

public class ClientRefreshException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 5391285827332471674L;

    public ClientRefreshException(String msg) {
        this(msg, null);
    }

    public ClientRefreshException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
