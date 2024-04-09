package org.meteor.client.exception;

import java.io.Serial;

public class MeteorClientException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 5391285827332471674L;

    public MeteorClientException(String msg) {
        this(msg, null);
    }

    public MeteorClientException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
