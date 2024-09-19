package org.meteor.common.logging;

import java.io.ObjectStreamException;
import java.io.Serial;
import java.io.Serializable;
import org.meteor.common.util.ObjectUtil;
import org.meteor.common.util.StringUtil;

/**
 * An abstract base class for implementing the InternalLogger interface, providing common logging functionalities.
 * This class ensures logging of messages and exceptions at various levels (TRACE, DEBUG, INFO, WARN, ERROR).
 * It also supports logging with formatted messages and multiple arguments.
 * <p>
 * The class implements Serializable interface and provides a readResolve method to ensure the correct logger is resolved during deserialization.
 * <p>
 * Field:
 * - EXCEPTION_MESSAGE: A static final String used as a default message when logging exceptions.
 * - serialVersionUID: A static final long for the serializable class identifier.
 * - name: A final String representing the name of the logger.
 * <p>
 * Methods:
 * - name(): Returns the name of the logger.
 * - isEnabled(InternalLogLevel level): Checks if the logger is enabled for the specified log level.
 * - trace(Throwable t): Logs a TRACE level message with an exception.
 * - debug(Throwable t): Logs a DEBUG level message with an exception.
 * - info(Throwable t): Logs an INFO level message with an exception.
 * - warn(Throwable t): Logs a WARN level message with an exception.
 * - error(Throwable t): Logs an ERROR level message with an exception.
 * - log(InternalLogLevel level, String msg, Throwable cause): Logs a message and an exception at the specified log level.
 * - log(InternalLogLevel level, Throwable cause): Logs an exception at the specified log level.
 * - log(InternalLogLevel level, String msg): Logs a message at the specified log level.
 * - log(InternalLogLevel level, String format, Object arg): Logs a formatted message with one argument at the specified log level.
 * - log(InternalLogLevel level, String format, Object argA, Object argB): Logs a formatted message with two arguments at the specified log level.
 * - log(InternalLogLevel level, String format, Object... arguments): Logs a formatted message with multiple arguments at the specified log level.
 * - readResolve(): Ensures the correct logger is resolved during deserialization.
 * - toString(): Returns the string representation of the logger, including its simple class name and name.
 */
public abstract class AbstractInternalLogger implements InternalLogger, Serializable {
    /**
     * A constant string used to prefix log messages for unexpected exceptions.
     */
    static final String EXCEPTION_MESSAGE = "Unexpected exception:";
    /**
     * A unique identifier for the Serializable class, used during the deserialization process
     * to verify that the sender and receiver of a serialized object have loaded classes
     * for that object that are compatible with respect to serialization.
     */
    @Serial
    private static final long serialVersionUID = -6382972526573193470L;
    /**
     * The name of the logger instance.
     */
    private final String name;

    /**
     * Constructor for the AbstractInternalLogger class.
     * Initializes the logger with the specified name.
     *
     * @param name The name of the logger. Must not be null.
     * @throws NullPointerException if the name is null.
     */
    protected AbstractInternalLogger(String name) {
        this.name = ObjectUtil.checkNotNull(name, "name");
    }

    /**
     * Returns the name of the logger.
     *
     * @return the name of the logger
     */
    @Override
    public String name() {
        return name;
    }

    /**
     * Determines if logging is enabled for the specified {@code InternalLogLevel}.
     *
     * @param level the log level to check
     * @return {@code true} if logging is enabled for the provided level, {@code false} otherwise
     */
    @Override
    public boolean isEnabled(InternalLogLevel level) {
        return switch (level) {
            case TRACE -> isTraceEnabled();
            case DEBUG -> isDebugEnabled();
            case INFO -> isInfoEnabled();
            case WARN -> isWarnEnabled();
            case ERROR -> isErrorEnabled();
        };
    }

    /**
     * Logs a throwable at the TRACE level.
     *
     * @param t the throwable to be logged
     */
    @Override
    public void trace(Throwable t) {
        trace(EXCEPTION_MESSAGE, t);
    }

    /**
     * Logs a debug message with an associated {@link Throwable}.
     *
     * @param t the {@link Throwable} instance to be logged.
     */
    @Override
    public void debug(Throwable t) {
        debug(EXCEPTION_MESSAGE, t);
    }

    /**
     * Logs an informational message along with a throwable.
     *
     * @param t the throwable to be logged
     */
    @Override
    public void info(Throwable t) {
        info(EXCEPTION_MESSAGE, t);
    }

    /**
     * Issues a warning log message with an exception.
     *
     * @param t the throwable to be logged as a warning
     */
    @Override
    public void warn(Throwable t) {
        warn(EXCEPTION_MESSAGE, t);
    }

    /**
     * Logs an error message with an associated Throwable.
     *
     * @param t The Throwable to log along with the error message.
     */
    @Override
    public void error(Throwable t) {
        error(EXCEPTION_MESSAGE, t);
    }

    /**
     * Logs a message with a specific log level and an optional throwable cause.
     *
     * @param level the log level at which the message should be recorded
     * @param msg the message to be logged
     * @param cause the throwable cause associated with the message, can be null
     */
    @Override
    public void log(InternalLogLevel level, String msg, Throwable cause) {
        switch (level) {
            case TRACE -> trace(msg, cause);
            case DEBUG -> debug(msg, cause);
            case INFO -> info(msg, cause);
            case WARN -> warn(msg, cause);
            case ERROR -> error(msg, cause);
            default -> throw new Error();
        }
    }

    /**
     * Logs a given Throwable at the specified log level.
     *
     * @param level the log level at which the Throwable should be logged
     * @param cause the Throwable to log
     */
    @Override
    public void log(InternalLogLevel level, Throwable cause) {
        switch (level) {
            case TRACE -> trace(cause);
            case DEBUG -> debug(cause);
            case INFO -> info(cause);
            case WARN -> warn(cause);
            case ERROR -> error(cause);
            default -> throw new Error();
        }
    }

    /**
     * Logs a message at the specified logging level.
     *
     * @param level the logging level at which to log the message
     * @param msg the message to be logged
     */
    @Override
    public void log(InternalLogLevel level, String msg) {
        switch (level) {
            case TRACE -> trace(msg);
            case DEBUG -> debug(msg);
            case INFO -> info(msg);
            case WARN -> warn(msg);
            case ERROR -> error(msg);
            default -> throw new Error();
        }
    }

    /**
     * Logs a message at the specified log level using the provided format and argument.
     *
     * @param level the log level at which the message should be logged
     * @param format the format string
     * @param arg the argument to be formatted
     */
    @Override
    public void log(InternalLogLevel level, String format, Object arg) {
        switch (level) {
            case TRACE -> trace(format, arg);
            case DEBUG -> debug(format, arg);
            case INFO -> info(format, arg);
            case WARN -> warn(format, arg);
            case ERROR -> error(format, arg);
            default -> throw new Error();
        }
    }

    /**
     * Logs a message at the specified log level using the given format and arguments.
     *
     * @param level the log level at which the message should be logged
     * @param format the format string
     * @param argA the first argument to be substituted into the format string
     * @param argB the second argument to be substituted into the format string
     */
    @Override
    public void log(InternalLogLevel level, String format, Object argA, Object argB) {
        switch (level) {
            case TRACE -> trace(format, argA, argB);
            case DEBUG -> debug(format, argA, argB);
            case INFO -> info(format, argA, argB);
            case WARN -> warn(format, argA, argB);
            case ERROR -> error(format, argA, argB);
            default -> throw new Error();
        }
    }

    /**
     * Logs a message at the specified log level with the given format and arguments.
     *
     * @param level the log level at which the message should be logged
     * @param format the format string
     * @param arguments the arguments referenced by the format string
     */
    @Override
    public void log(InternalLogLevel level, String format, Object... arguments) {
        switch (level) {
            case TRACE -> trace(format, arguments);
            case DEBUG -> debug(format, arguments);
            case INFO -> info(format, arguments);
            case WARN -> warn(format, arguments);
            case ERROR -> error(format, arguments);
            default -> throw new Error();
        }
    }

    /**
     * Resolves the deserialized object to ensure the singleton property and
     * maintain the logger reference.
     *
     * @return an instance of InternalLogger based on the current name.
     * @throws ObjectStreamException if an object stream exception occurs.
     */
    @Serial
    protected Object readResolve() throws ObjectStreamException {
        return InternalLoggerFactory.getLogger(name());
    }

    /**
     * Returns a string representation of the current logger instance.
     *
     * @return a string consisting of the simple class name and the logger name.
     */
    @Override
    public String toString() {
        return StringUtil.simpleClassName(this) + '(' + name() + ')';
    }
}
