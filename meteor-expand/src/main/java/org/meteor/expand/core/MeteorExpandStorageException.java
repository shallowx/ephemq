package org.meteor.expand.core;

public class MeteorExpandStorageException extends RuntimeException {

    public MeteorExpandStorageException() {
        super();
    }

    public MeteorExpandStorageException(String message) {
        super(message);
    }

    public MeteorExpandStorageException(String message, Throwable cause) {
        super(message, cause);
    }

    public MeteorExpandStorageException(Throwable cause) {
        super(cause);
    }

    public MeteorExpandStorageException(String message, Throwable cause, boolean enableSuppression,
                                        boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
