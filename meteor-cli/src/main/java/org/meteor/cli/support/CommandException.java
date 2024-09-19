package org.meteor.cli.support;

import java.io.Serial;

/**
 * Represents an exception that occurs when a command cannot be executed successfully.
 * This exception is thrown to indicate various issues that may arise during the execution
 * of a command, such as invalid arguments or internal processing errors.
 * <p>
 * The {@code CommandException} extends {@code RuntimeException}, which means it is
 * an unchecked exception and does not require explicit handling in the code.
 * <p>
 * Constructors:
 * - {@code CommandException(String message)}: Constructs a new exception with a specified detail message.
 * - {@code CommandException(String message, Throwable cause)}: Constructs a new exception with a specified detail message
 * and a cause.
 * <p>
 * Example usage scenarios include errors during the execution of command-line interface commands
 * for different operations, such as querying cluster information, creating topics, and migrating ledgers.
 */
public class CommandException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 6926716840699621852L;

    /**
     * Constructs a new CommandException with a specified detail message.
     *
     * @param message the detail message explaining the reason for the exception.
     */
    public CommandException(String message) {
        super(message);
    }

    /**
     * Constructs a new CommandException with the specified detail message and cause.
     *
     * @param message the detailed error message.
     * @param cause the cause of the exception (which is saved for later retrieval by the {@link Throwable#getCause()} method).
     *              A null value is permitted and indicates that the cause is nonexistent or unknown.
     */
    public CommandException(String message, Throwable cause) {
        super(message, cause);
    }
}
