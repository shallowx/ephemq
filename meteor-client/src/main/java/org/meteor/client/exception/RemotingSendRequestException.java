package org.meteor.client.exception;

import java.io.Serial;

public class RemotingSendRequestException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 5391285827332471674L;

    public RemotingSendRequestException(String addr) {
        this(addr, null);
    }

    public RemotingSendRequestException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
