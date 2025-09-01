package org.ephemq.remote.exception;

import java.io.Serial;

/**
 * Exception thrown to indicate a problem during a remoting operation.
 * This is a custom runtime exception that includes an error code to specify the type of failure.
 */
public final class RemotingException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 6926716840699621852L;

    /**
     * The error code associated with the failure type.
     * This code identifies the specific type of remoting error that occurred.
     */
    private final int command;

    /**
     * Constructs a new RemotingException with the specified command and error message.
     *
     * @param command the command or error code indicating the type of failure.
     * @param error   the detail message to be used for the exception.
     */
    public RemotingException(int command, String error) {
        super(error);
        this.command = command;
    }

    /**
     * Constructs a new RemotingException with the specified command, error message, and cause.
     *
     * @param command the command associated with the exception
     * @param error the detail error message
     * @param cause the cause of the exception
     */
    public RemotingException(int command, String error, Throwable cause) {
        super(error, cause);
        this.command = command;
    }

    /**
     * Creates a new RemotingException instance with the specified command and error message.
     * If the provided command is not a negative value, the command will default to Failure.UNKNOWN_EXCEPTION.
     *
     * @param command The command code representing the type of failure. Must be a negative integer.
     * @param error The error message associated with the exception.
     * @return A new instance of RemotingException with the specified command and error message.
     */
    public static RemotingException of(int command, String error) {
        command = command < 0 ? command : Failure.UNKNOWN_EXCEPTION;
        return new RemotingException(command, error);
    }

    /**
     * Creates a new instance of RemotingException with the specified command, error message, and cause.
     * If the command is non-negative, it is replaced with {@code Failure.UNKNOWN_EXCEPTION}.
     *
     * @param command The specific error command. If non-negative, it is set to {@code Failure.UNKNOWN_EXCEPTION}.
     * @param error The error message to associate with this exception.
     * @param cause The cause of the exception.
     * @return A new instance of RemotingException.
     */
    public static RemotingException of(int command, String error, Throwable cause) {
        command = command < 0 ? command : Failure.UNKNOWN_EXCEPTION;
        return new RemotingException(command, error, cause);
    }

    /**
     * Gets the error code associated with the exception.
     *
     * @return the command error code.
     */
    public int getCommand() {
        return command;
    }

    /**
     * Overrides the default behavior of {@code Throwable}'s {@code fillInStackTrace} method.
     * This implementation returns the current instance without modifying the stack trace.
     *
     * @return this {@code RemotingException} instance
     */
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
