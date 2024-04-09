package org.meteor.remote.exception;

import java.io.Serial;

public class RemotingDecoderException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 6926716840699621852L;

    public RemotingDecoderException() {
    }

    public RemotingDecoderException(String message, Throwable cause) {
        super(message, cause);
    }

    public RemotingDecoderException(String message) {
        super(message);
    }

    public RemotingDecoderException(Throwable cause) {
        super(cause);
    }
}
