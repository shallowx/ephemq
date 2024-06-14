package org.meteor.remote.exception;

import java.io.Serial;

public final class RemotingException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 6926716840699621852L;

    private final int command;

    public RemotingException(int command, String error) {
        super(error);
        this.command = command;
    }

    public RemotingException(int command, String error, Throwable cause) {
        super(error, cause);
        this.command = command;
    }

    public static RemotingException of(int command, String error) {
        command = command < 0 ? command : Failure.UNKNOWN_EXCEPTION;
        return new RemotingException(command, error);
    }

    public static RemotingException of(int command, String error, Throwable cause) {
        command = command < 0 ? command : Failure.UNKNOWN_EXCEPTION;
        return new RemotingException(command, error, cause);
    }

    public int getCommand() {
        return command;
    }

    @Override
    public Throwable fillInStackTrace() {
        return this;
    }

    public interface Failure {
        int UNKNOWN_EXCEPTION = -1;
        int UNSUPPORTED_EXCEPTION = -2;
        int PROCESS_EXCEPTION = -3;
    }
}
