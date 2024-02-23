package org.meteor.remote.invoke;

public final class RemoteException extends RuntimeException {
    private final int command;
    public RemoteException(int command, String error) {
        super(error);
        this.command = command;
    }

    public RemoteException(int command, String error, Throwable cause) {
        super(error, cause);
        this.command = command;
    }

    public static RemoteException of(int command, String error) {
        command = command < 0 ? command : Failure.UNKNOWN_EXCEPTION;
        return new RemoteException(command, error);
    }

    public static RemoteException of(int command, String error, Throwable cause) {
        command = command < 0 ? command : Failure.UNKNOWN_EXCEPTION;
        return new RemoteException(command, error, cause);
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
        int INVOKE_TIMEOUT_EXCEPTION = -2;
        int UNSUPPORTED_EXCEPTION = -3;
        int PROCESS_EXCEPTION = -4;
    }
}
