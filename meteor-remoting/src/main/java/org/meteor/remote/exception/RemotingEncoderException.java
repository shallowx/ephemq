package org.meteor.remote.exception;

import java.io.Serial;

public class RemotingEncoderException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 6926716840699621852L;

    public RemotingEncoderException() {
    }

    public RemotingEncoderException(String message, Throwable cause) {
        super(message, cause);
    }

    public RemotingEncoderException(String message) {
        super(message);
    }

    public RemotingEncoderException(Throwable cause) {
        super(cause);
    }
}
