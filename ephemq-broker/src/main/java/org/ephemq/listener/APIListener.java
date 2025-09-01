package org.ephemq.listener;

/**
 * APIListener is an interface for handling command execution events.
 * Implementations of this interface can be registered to listen for command events
 * within a system, allowing for custom behavior to be executed when a command is processed.
 * <p>
 * Methods:
 * - void onCommand(int code, int bytes, long cost, boolean result):
 * Called when a command is executed, providing details about the command execution such as the command code,
 * the number of bytes processed, the time taken to execute, and the result of the execution.
 */
public interface APIListener {
    /**
     * Called when a command is executed, providing detailed information about the execution.
     *
     * @param code   The code identifying the command that was executed.
     * @param bytes  The number of bytes processed during the execution of the command.
     * @param cost   The time taken to execute the command, measured in milliseconds.
     * @param result The result of the command's execution; true if successful, false otherwise.
     */
    void onCommand(int code, int bytes, long cost, boolean result);
}
