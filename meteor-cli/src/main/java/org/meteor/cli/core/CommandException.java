package org.meteor.cli.core;

import java.io.Serial;

public class CommandException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 6926716840699621852L;

    public CommandException(String message) {
        super(message);
    }

    public CommandException(String message, Throwable cause) {
        super(message, cause);
    }
}
