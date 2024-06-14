package org.meteor.exception;

import java.io.Serial;

public class TopicHandleException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 5391285827332471674L;

    public TopicHandleException(String msg) {
        this(msg, null);
    }

    public TopicHandleException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
