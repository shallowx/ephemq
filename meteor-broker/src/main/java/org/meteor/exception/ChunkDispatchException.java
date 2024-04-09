package org.meteor.exception;

import java.io.Serial;

public class ChunkDispatchException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 5391285827332471674L;

    public ChunkDispatchException(String msg) {
        this(msg, null);
    }

    public ChunkDispatchException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
