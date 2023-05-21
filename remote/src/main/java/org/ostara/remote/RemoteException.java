package org.ostara.remote;

public final class RemoteException extends RuntimeException {

    private final int command;

    public static RemoteException of(int command, String error) {
        command = command < 0 ? command : Failure.UNKNOWN_EXCEPTION;
        return new RemoteException(command, error);
    }

    public static RemoteException of(int command, String error, Throwable cause) {
        command = command < 0 ? command : Failure.UNKNOWN_EXCEPTION;
        return new RemoteException(command, error, cause);
    }

    public RemoteException(int command, String error) {
        super(error);
        this.command = command;
    }

    public RemoteException(int command, String error, Throwable cause) {
        super(error, cause);
        this.command = command;
    }

    public int getCommand() {
        return command;
    }

    @Override
    public Throwable fillInStackTrace() {
        return this;
    }

    public interface Failure {
        byte UNKNOWN_EXCEPTION = -1;
        byte INVOKE_TIMEOUT_EXCEPTION = -2;
        byte UNSUPPORTED_EXCEPTION = -3;
        byte MESSAGE_APPEND_EXCEPTION = -4;
        byte SUBSCRIBE_EXCEPTION = -5;
    }
}
