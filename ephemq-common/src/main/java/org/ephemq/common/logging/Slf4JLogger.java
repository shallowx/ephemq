package org.ephemq.common.logging;

import org.slf4j.Logger;

import java.io.Serial;

/**
 * Slf4JLogger is a concrete implementation of AbstractInternalLogger that utilizes
 * the SLF4J framework for logging. This class provides methods for logging messages
 * at various levels such as TRACE, DEBUG, INFO, WARN, and ERROR.
 * <p>
 * It delegates the logging functionalities to an SLF4J Logger instance.
 * Methods are provided to log messages with different verbosity and to check
 * if a certain logging level is enabled.
 */
final class Slf4JLogger extends AbstractInternalLogger {
    /**
     * A unique identifier for Serializable classes used to verify
     * the sender and receiver of a serialized object are compatible
     * with respect to serialization.
     */
    @Serial
    private static final long serialVersionUID = 108038972685130825L;
    /**
     * The logger instance used for logging messages.
     * This logger is specific to the Slf4JLogger class and
     * provides various logging functionalities like trace,
     * debug, info, warn, and error.
     * <p>
     * This instance is marked as transient, meaning it
     * will not be serialized when the Slf4JLogger object
     * is converted into a byte stream.
     */
    private final transient Logger logger;

    /**
     * Constructs a new Slf4JLogger instance that wraps the provided SLF4J Logger.
     *
     * @param logger the SLF4J Logger to be wrapped. Must not be null.
     */
    Slf4JLogger(Logger logger) {
        super(logger.getName());
        this.logger = logger;
    }

    /**
     * Checks if trace logging is enabled.
     *
     * @return true if trace logging is enabled, false otherwise.
     */
    @Override
    public boolean isTraceEnabled() {
        return logger.isTraceEnabled();
    }

    /**
     * Logs a trace level message.
     *
     * @param msg the message to be logged
     */
    @Override
    public void trace(String msg) {
        logger.trace(msg);
    }

    /**
     * Logs a trace message with a specified format and a single argument.
     *
     * @param format the string format to be used
     * @param arg the argument to format using the specified format
     */
    @Override
    public void trace(String format, Object arg) {
        logger.trace(format, arg);
    }

    /**
     * Logs a trace message formatted using the specified format string with two arguments.
     *
     * @param format the string format to be used for the message
     * @param argA the first argument to be used in the message
     * @param argB the second argument to be used in the message
     */
    @Override
    public void trace(String format, Object argA, Object argB) {
        logger.trace(format, argA, argB);
    }

    /**
     * Logs a TRACE level message with a formatted string and accompanying arguments.
     *
     * @param format the string format following java.util.Formatter syntax.
     * @param argArray an array of objects to be formatted and included in the log message.
     */
    @Override
    public void trace(String format, Object... argArray) {
        logger.trace(format, argArray);
    }

    /**
     * Logs a trace message along with a throwable.
     *
     * @param msg The trace message to be logged.
     * @param t The throwable associated with the trace message.
     */
    @Override
    public void trace(String msg, Throwable t) {
        logger.trace(msg, t);
    }

    /**
     * Checks if the debug level logging is enabled for this logger.
     *
     * @return {@code true} if the debug level is enabled, {@code false} otherwise.
     */
    @Override
    public boolean isDebugEnabled() {
        return logger.isDebugEnabled();
    }

    /**
     * Logs a debug message using the provided logger.
     *
     * @param msg The debug message to log.
     */
    @Override
    public void debug(String msg) {
        logger.debug(msg);
    }

    /**
     * Logs a message at the DEBUG level according to the specified format and argument.
     *
     * @param format the format string
     * @param arg the argument to be formatted and substituted in the format string
     */
    @Override
    public void debug(String format, Object arg) {
        logger.debug(format, arg);
    }

    /**
     * Logs a debug message with a specific format and two arguments.
     *
     * @param format the string format following SLF4J format patterns
     * @param argA the first argument to be replaced in the format string
     * @param argB the second argument to be replaced in the format string
     */
    @Override
    public void debug(String format, Object argA, Object argB) {
        logger.debug(format, argA, argB);
    }

    /**
     * Logs a debug message with a specific format and arguments.
     *
     * @param format the string containing the format to be used
     * @param argArray an array of arguments to be used within the formatted message
     */
    @Override
    public void debug(String format, Object... argArray) {
        logger.debug(format, argArray);
    }

    /**
     * Logs a debug message with an associated throwable.
     *
     * @param msg the debug message to log
     * @param t the throwable to log with the debug message
     */
    @Override
    public void debug(String msg, Throwable t) {
        logger.debug(msg, t);
    }

    /**
     * Checks if info level logging is enabled in the logger.
     *
     * @return true if info level is enabled, false otherwise.
     */
    @Override
    public boolean isInfoEnabled() {
        return logger.isInfoEnabled();
    }

    /**
     * Logs a message at the INFO level.
     *
     * @param msg the message string to be logged
     */
    @Override
    public void info(String msg) {
        logger.info(msg);
    }

    /**
     * Logs an informational message with the specified format and argument.
     *
     * @param format the format string
     * @param arg the argument to be formatted
     */
    @Override
    public void info(String format, Object arg) {
        logger.info(format, arg);
    }

    /**
     * Logs a message at the INFO level according to the specified format and arguments.
     *
     * @param format the string message format
     * @param argA the first argument to format
     * @param argB the second argument to format
     */
    @Override
    public void info(String format, Object argA, Object argB) {
        logger.info(format, argA, argB);
    }

    /**
     * Logs an information message with a specific format and argument array.
     *
     * @param format the format string
     * @param argArray the arguments to be formatted into the message
     */
    @Override
    public void info(String format, Object... argArray) {
        logger.info(format, argArray);
    }

    /**
     * Logs an info message along with a throwable.
     *
     * @param msg the message string to be logged
     * @param t the throwable to be logged
     */
    @Override
    public void info(String msg, Throwable t) {
        logger.info(msg, t);
    }

    /**
     * Checks whether warn level logging is enabled.
     *
     * @return true if warn level logging is enabled, false otherwise.
     */
    @Override
    public boolean isWarnEnabled() {
        return logger.isWarnEnabled();
    }

    /**
     * Logs a warning message using the configured logger.
     *
     * @param msg the message to be logged as a warning
     */
    @Override
    public void warn(String msg) {
        logger.warn(msg);
    }

    /**
     * Logs a warning message using a specified format and a single argument.
     *
     * @param format the format string
     * @param arg the argument to be applied to the format string
     */
    @Override
    public void warn(String format, Object arg) {
        logger.warn(format, arg);
    }

    /**
     * Logs a warning message with a specific format and optional arguments.
     *
     * @param format the format string
     * @param argArray the arguments to be substituted into the format string
     */
    @Override
    public void warn(String format, Object... argArray) {
        logger.warn(format, argArray);
    }

    /**
     * Logs a warning message with the specified format string and two arguments.
     *
     * @param format the format string
     * @param argA the first argument
     * @param argB the second argument
     */
    @Override
    public void warn(String format, Object argA, Object argB) {
        logger.warn(format, argA, argB);
    }

    /**
     * Logs a warning message with an associated {@code Throwable}.
     *
     * @param msg the warning message to log
     * @param t the {@code Throwable} associated with the warning
     */
    @Override
    public void warn(String msg, Throwable t) {
        logger.warn(msg, t);
    }

    /**
     * Checks whether error level logging is enabled.
     *
     * @return true if error level logging is enabled, false otherwise
     */
    @Override
    public boolean isErrorEnabled() {
        return logger.isErrorEnabled();
    }

    /**
     * Logs a message at the error level.
     *
     * @param msg the message string to be logged
     */
    @Override
    public void error(String msg) {
        logger.error(msg);
    }

    /**
     * Logs an error message with a single argument.
     *
     * @param format the format string
     * @param arg the argument to be inserted into the format string
     */
    @Override
    public void error(String format, Object arg) {
        logger.error(format, arg);
    }

    /**
     * Logs an error message with the specified format and two arguments.
     *
     * @param format the format string
     * @param argA the first argument
     * @param argB the second argument
     */
    @Override
    public void error(String format, Object argA, Object argB) {
        logger.error(format, argA, argB);
    }

    /**
     * Logs an error message with an optional list of arguments.
     *
     * @param format the format string
     * @param argArray the arguments to be formatted and logged
     */
    @Override
    public void error(String format, Object... argArray) {
        logger.error(format, argArray);
    }

    /**
     * Logs an error message along with a Throwable instance.
     *
     * @param msg the error message to be logged
     * @param t the Throwable instance to be logged
     */
    @Override
    public void error(String msg, Throwable t) {
        logger.error(msg, t);
    }
}
