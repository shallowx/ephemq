package org.meteor.remote.exception;

import java.io.Serial;

public class RemotingTimeoutException extends Exception {

    @Serial
    private static final long serialVersionUID = 4106899185095245979L;

    public RemotingTimeoutException(String message) {
        super(message);
    }

    public RemotingTimeoutException(String addr, long timeoutMillis) {
        this(addr, timeoutMillis, null);
    }

    public RemotingTimeoutException(String addr, long timeoutMillis, Throwable cause) {
        super("Wait response on the channel <" + addr + "> timeout, " + timeoutMillis + "(ms)", cause);
    }
}
