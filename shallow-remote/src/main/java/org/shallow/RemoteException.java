package org.shallow;

import static org.shallow.ObjectUtil.checkNegative;

public final class RemoteException extends RuntimeException {

    private byte command;

    public static RemoteException of(byte command, String error) {
        command = command < 0 ? command : Failure.UNKNOWN_EXCEPTION;
        return new RemoteException(command, error);
    }

    public static RemoteException of(byte command, String error, Throwable cause) {
        command = command < 0 ? command : Failure.UNKNOWN_EXCEPTION;
        return new RemoteException(command, error, cause);
    }

    public RemoteException(byte command, String error) {
        super(error);
        this.command = (byte) checkNegative(command, "command");
    }

    public RemoteException(byte command, String error, Throwable cause) {
        super(error, cause);
        this.command = (byte) checkNegative(command, "command");
    }

    public byte getCommand() {
        return command;
    }

    @Override
    public Throwable fillInStackTrace() {
        return this;
    }

    public interface Failure {
        byte UNKNOWN_EXCEPTION = 1;
        byte INVOKE_TIMEOUT_EXCEPTION = 2;
    }
}
