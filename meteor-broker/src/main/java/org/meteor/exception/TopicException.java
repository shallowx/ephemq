package org.meteor.exception;

import java.io.Serial;

public class TopicException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 5391285827332471674L;

    public TopicException(String msg) {
        this(msg, null);
    }

    public TopicException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
