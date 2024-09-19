package org.meteor.common.logging;

import java.io.Serial;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Concrete implementation of the AbstractInternalLogger using Log4J.
 * <p>
 * This class wraps around an existing Log4J {@link Logger} instance
 * and provides the logging functions such as trace, debug, info, warn, and error.
 */
class Log4JLogger extends AbstractInternalLogger {
    /**
     * Fully Qualified Class Name (FQCN) of the Log4JLogger class.
     * This field is typically used for logging and tracing purposes.
     */
    static final String FQCN = Log4JLogger.class.getName();
    /**
     * The serialVersionUID is a unique identifier for Serializable classes.
     * It is used during the deserialization process to verify that
     * the sender and receiver of a serialized object have loaded classes
     * for that object that are compatible with respect to serialization.
     */
    @Serial
    private static final long serialVersionUID = 2851357342488183058L;
    /**
     * The Logger instance used for logging within the Log4JLogger class.
     * This Logger is initialized during the creation of Log4JLogger and
     * used across various logging methods to output trace, debug, info,
     * warn, and error messages.
     */
    final transient Logger logger;
    /**
     * Indicates whether the logger instance is capable of handling trace level log messages.
     *
     * This field is used internally to check if trace level logging is supported by the underlying logger.
     */
    final boolean traceCapable;

    /**
     * Constructs a new Log4JLogger instance wrapping the provided Logger object.
     *
     * @param logger The Logger instance to be wrapped by this Log4JLogger.
     */
    Log4JLogger(Logger logger) {
        super(logger.getName());
        this.logger = logger;
        traceCapable = isTraceCapable();
    }

    /**
     * Determines if the underlying logging framework supports trace level logging.
     *
     * @return {@code true} if trace level logging is supported, otherwise {@code false}
     */
    private boolean isTraceCapable() {
        try {
            logger.isTraceEnabled();
            return true;
        } catch (NoSuchMethodError ignored) {
            return false;
        }
    }

    /**
     * Checks if trace level logging is enabled.
     * If the logger is trace capable, it checks if trace level logging is enabled.
     * Otherwise, it checks if debug level logging is enabled as a fallback.
     *
     * @return true if trace level logging is enabled, false otherwise
     */
    @Override
    public boolean isTraceEnabled() {
        if (traceCapable) {
            return logger.isTraceEnabled();
        } else {
            return logger.isDebugEnabled();
        }
    }

    /**
     * Logs a trace message. If trace capability is enabled, it logs with TRACE level, otherwise it logs with DEBUG level.
     *
     * @param msg the message string to be logged
     */
    @Override
    public void trace(String msg) {
        logger.log(FQCN, traceCapable ? Level.TRACE : Level.DEBUG, msg, null);
    }

    /**
     * Logs a trace message, formatted with a single argument, if trace logging is enabled.
     *
     * @param format the string format of the message to be logged, containing placeholders for formatting
     * @param arg the argument to be substituted into the format string
     */
    @Override
    public void trace(String format, Object arg) {
        if (isTraceEnabled()) {
            FormattingTuple ft = MessageFormatter.format(format, arg);
            logger.log(FQCN, traceCapable ? Level.TRACE : Level.DEBUG, ft
                    .getMessage(), ft.getThrowable());
        }
    }

    /**
     * Logs a trace message with formatting and support for two arguments.
     *
     * @param format the string format to be applied to the arguments
     * @param argA the first argument to be applied to the format string
     * @param argB the second argument to be applied to the format string
     */
    @Override
    public void trace(String format, Object argA, Object argB) {
        if (isTraceEnabled()) {
            FormattingTuple ft = MessageFormatter.format(format, argA, argB);
            logger.log(FQCN, traceCapable ? Level.TRACE : Level.DEBUG, ft
                    .getMessage(), ft.getThrowable());
        }
    }

    /**
     * Logs a trace message with optional arguments if trace logging is enabled.
     *
     * @param format the format string
     * @param arguments the arguments referenced by the format specifiers in the format string
     */
    @Override
    public void trace(String format, Object... arguments) {
        if (isTraceEnabled()) {
            FormattingTuple ft = MessageFormatter.arrayFormat(format, arguments);
            logger.log(FQCN, traceCapable ? Level.TRACE : Level.DEBUG, ft
                    .getMessage(), ft.getThrowable());
        }
    }

    /**
     * Logs a given message with an optional throwable at the trace level.
     * If trace logging is not supported, falls back to debug level.
     *
     * @param msg the message to log
     * @param t the throwable associated with the message, may be null
     */
    @Override
    public void trace(String msg, Throwable t) {
        logger.log(FQCN, traceCapable ? Level.TRACE : Level.DEBUG, msg, t);
    }

    /**
     * Checks if the logger is enabled for the DEBUG level.
     *
     * @return true if the DEBUG level is enabled, false otherwise.
     */
    @Override
    public boolean isDebugEnabled() {
        return logger.isDebugEnabled();
    }

    /**
     * Logs a message at the DEBUG level.
     *
     * @param msg the message string to be logged
     */
    @Override
    public void debug(String msg) {
        logger.log(FQCN, Level.DEBUG, msg, null);
    }

    /**
     * Logs a debug message with a single argument if debug logging is enabled.
     *
     * @param format the format string containing placeholders
     * @param arg the argument to be substituted into the format string
     */
    @Override
    public void debug(String format, Object arg) {
        if (logger.isDebugEnabled()) {
            FormattingTuple ft = MessageFormatter.format(format, arg);
            logger.log(FQCN, Level.DEBUG, ft.getMessage(), ft.getThrowable());
        }
    }

    /**
     * Logs a debug message with a formatted string and two arguments.
     * If debug logging is enabled, the message is formatted using the provided format and arguments,
     * and then logged at the DEBUG level.
     *
     * @param format the format string
     * @param argA the first argument to be formatted and inserted into the message
     * @param argB the second argument to be formatted and inserted into the message
     */
    @Override
    public void debug(String format, Object argA, Object argB) {
        if (logger.isDebugEnabled()) {
            FormattingTuple ft = MessageFormatter.format(format, argA, argB);
            logger.log(FQCN, Level.DEBUG, ft.getMessage(), ft.getThrowable());
        }
    }

    /**
     * Logs a debug message with optional format and arguments. This method constructs the message
     * using the specified format and arguments if debug level logging is enabled.
     *
     * @param format the format string
     * @param arguments the list of arguments to be formatted into the message
     */
    @Override
    public void debug(String format, Object... arguments) {
        if (logger.isDebugEnabled()) {
            FormattingTuple ft = MessageFormatter.arrayFormat(format, arguments);
            logger.log(FQCN, Level.DEBUG, ft.getMessage(), ft.getThrowable());
        }
    }

    /**
     * Logs a debug level message along with an exception.
     *
     * @param msg the message to be logged
     * @param t the exception to be logged
     */
    @Override
    public void debug(String msg, Throwable t) {
        logger.log(FQCN, Level.DEBUG, msg, t);
    }

    /**
     * Checks if the INFO level logging is enabled.
     *
     * @return true if INFO level logging is enabled, false otherwise.
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
        logger.log(FQCN, Level.INFO, msg, null);
    }

    /**
     * Logs an informational message formatted with one argument if the info level is enabled.
     *
     * @param format the format string
     * @param arg the argument to be formatted and included in the message
     */
    @Override
    public void info(String format, Object arg) {
        if (logger.isInfoEnabled()) {
            FormattingTuple ft = MessageFormatter.format(format, arg);
            logger.log(FQCN, Level.INFO, ft.getMessage(), ft.getThrowable());
        }
    }

    /**
     * Logs a message at the INFO level using the specified format string and arguments.
     *
     * @param format the format string
     * @param argA the first argument
     * @param argB the second argument
     */
    @Override
    public void info(String format, Object argA, Object argB) {
        if (logger.isInfoEnabled()) {
            FormattingTuple ft = MessageFormatter.format(format, argA, argB);
            logger.log(FQCN, Level.INFO, ft.getMessage(), ft.getThrowable());
        }
    }

    /**
     * Logs a formatted informational message with optional arguments.
     *
     * @param format the format string
     * @param argArray the array of arguments to be used within the format string
     */
    @Override
    public void info(String format, Object... argArray) {
        if (logger.isInfoEnabled()) {
            FormattingTuple ft = MessageFormatter.arrayFormat(format, argArray);
            logger.log(FQCN, Level.INFO, ft.getMessage(), ft.getThrowable());
        }
    }

    /**
     * Logs an informational message including a throwable.
     *
     * @param msg the informational message to log
     * @param t the throwable to log
     */
    @Override
    public void info(String msg, Throwable t) {
        logger.log(FQCN, Level.INFO, msg, t);
    }

    /**
     * Checks if warn level logging is enabled.
     *
     * @return true if warn level logging is enabled, false otherwise.
     */
    @Override
    public boolean isWarnEnabled() {
        return logger.isEnabledFor(Level.WARN);
    }

    /**
     * Logs a warning message with the specified content.
     *
     * @param msg the warning message to be logged
     */
    @Override
    public void warn(String msg) {
        logger.log(FQCN, Level.WARN, msg, null);
    }

    /**
     * Logs a warning message with the specified format and argument.
     *
     * @param format the string format for the log message
     * @param arg the argument to be included in the formatted message
     */
    @Override
    public void warn(String format, Object arg) {
        if (logger.isEnabledFor(Level.WARN)) {
            FormattingTuple ft = MessageFormatter.format(format, arg);
            logger.log(FQCN, Level.WARN, ft.getMessage(), ft.getThrowable());
        }
    }

    /**
     * Logs a formatted warning message using the provided arguments.
     *
     * @param format the string format to be used
     * @param argA the first argument to be formatted
     * @param argB the second argument to be formatted
     */
    @Override
    public void warn(String format, Object argA, Object argB) {
        if (logger.isEnabledFor(Level.WARN)) {
            FormattingTuple ft = MessageFormatter.format(format, argA, argB);
            logger.log(FQCN, Level.WARN, ft.getMessage(), ft.getThrowable());
        }
    }

    /**
     * Logs a warning message using the specified format and arguments.
     * The warning message will only be logged if the logger is enabled for the WARN level.
     *
     * @param format the message format string
     * @param argArray the arguments to be replaced in the format string
     */
    @Override
    public void warn(String format, Object... argArray) {
        if (logger.isEnabledFor(Level.WARN)) {
            FormattingTuple ft = MessageFormatter.arrayFormat(format, argArray);
            logger.log(FQCN, Level.WARN, ft.getMessage(), ft.getThrowable());
        }
    }

    /**
     * Logs a warning message along with a throwable.
     *
     * @param msg the warning message to log
     * @param t the throwable to log with the warning message
     */
    @Override
    public void warn(String msg, Throwable t) {
        logger.log(FQCN, Level.WARN, msg, t);
    }

    /**
     * Checks whether error logging is enabled.
     *
     * @return true if error logging is enabled, false otherwise
     */
    @Override
    public boolean isErrorEnabled() {
        return logger.isEnabledFor(Level.ERROR);
    }

    /**
     * Logs an error message.
     *
     * @param msg The error message to log.
     */
    @Override
    public void error(String msg) {
        logger.log(FQCN, Level.ERROR, msg, null);
    }

    /**
     * Logs an error message with an optional single argument.
     *
     * @param format the format string
     * @param arg the argument to format the message with
     */
    @Override
    public void error(String format, Object arg) {
        if (logger.isEnabledFor(Level.ERROR)) {
            FormattingTuple ft = MessageFormatter.format(format, arg);
            logger.log(FQCN, Level.ERROR, ft.getMessage(), ft.getThrowable());
        }
    }

    /**
     * Logs an error message using the provided format and arguments if the error level is enabled.
     *
     * @param format the format string
     * @param argA the first argument to be substituted in the format string
     * @param argB the second argument to be substituted in the format string
     */
    @Override
    public void error(String format, Object argA, Object argB) {
        if (logger.isEnabledFor(Level.ERROR)) {
            FormattingTuple ft = MessageFormatter.format(format, argA, argB);
            logger.log(FQCN, Level.ERROR, ft.getMessage(), ft.getThrowable());
        }
    }

    /**
     * Logs an error message with an optional list of arguments.
     *
     * @param format the error message format string
     * @param argArray the arguments to be formatted into the message
     */
    @Override
    public void error(String format, Object... argArray) {
        if (logger.isEnabledFor(Level.ERROR)) {
            FormattingTuple ft = MessageFormatter.arrayFormat(format, argArray);
            logger.log(FQCN, Level.ERROR, ft.getMessage(), ft.getThrowable());
        }
    }

    /**
     * Logs an error message with an associated throwable.
     *
     * @param msg the error message to be logged
     * @param t the throwable associated with the error
     */
    @Override
    public void error(String msg, Throwable t) {
        logger.log(FQCN, Level.ERROR, msg, t);
    }
}
