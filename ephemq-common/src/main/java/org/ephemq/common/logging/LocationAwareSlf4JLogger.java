package org.ephemq.common.logging;

import static org.slf4j.spi.LocationAwareLogger.DEBUG_INT;
import static org.slf4j.spi.LocationAwareLogger.ERROR_INT;
import static org.slf4j.spi.LocationAwareLogger.INFO_INT;
import static org.slf4j.spi.LocationAwareLogger.TRACE_INT;
import static org.slf4j.spi.LocationAwareLogger.WARN_INT;
import java.io.Serial;
import org.slf4j.spi.LocationAwareLogger;

/**
 * A logger implementation that extends {@link AbstractInternalLogger} and provides location-aware logging features.
 * This class delegates logging operations to an underlying SLF4J {@link LocationAwareLogger}.
 * <p>
 * The {@code LocationAwareSlf4JLogger} is designed to support logging at various levels (TRACE, DEBUG, INFO, WARN, ERROR)
 * with optional formatting arguments and exception cause support.
 * <p>
 * The fully qualified class name (FQCN) is used to ensure correct location information is logged by the SLF4J framework.
 */
final class LocationAwareSlf4JLogger extends AbstractInternalLogger {
    /**
     * Fully Qualified Class Name (FQCN) for the LocationAwareSlf4JLogger.
     * This constant holds the name of the class as a string, which can be
     * used for logging purposes to identify the logger's class name.
     */
    static final String FQCN = LocationAwareSlf4JLogger.class.getName();
    /**
     * Serial version UID for ensuring compatibility during serialization and deserialization
     * of {@code LocationAwareSlf4JLogger} instances.
     */
    @Serial
    private static final long serialVersionUID = -8292030083201538180L;

    /**
     * A logger that is aware of its location, used for logging messages
     * with various levels of severity such as trace, debug, info, warn, and error.
     * This logger is transient and cannot be serialized.
     */
    private final transient LocationAwareLogger logger;

    /**
     * Constructs a LocationAwareSlf4JLogger instance with the specified LocationAwareLogger.
     *
     * @param logger the specified LocationAwareLogger instance.
     */
    LocationAwareSlf4JLogger(LocationAwareLogger logger) {
        super(logger.getName());
        this.logger = logger;
    }

    /**
     * Logs a message at the given level.
     *
     * @param level the severity level of the log message
     * @param message the message to log
     */
    private void log(final int level, final String message) {
        logger.log(null, FQCN, level, message, null, null);
    }

    /**
     * Logs a message at the specified logging level with an optional throwable cause.
     *
     * @param level   the logging level to use
     * @param message the message to log
     * @param cause   the throwable to log, which can be null
     */
    private void log(final int level, final String message, Throwable cause) {
        logger.log(null, FQCN, level, message, null, cause);
    }

    /**
     * Logs a message at the specified level with detailed formatting.
     *
     * @param level the logging level (e.g., DEBUG, INFO, WARN, ERROR)
     * @param tuple the message and related details to be logged, encapsulated in a {@link org.slf4j.helpers.FormattingTuple} object
     */
    private void log(final int level, final org.slf4j.helpers.FormattingTuple tuple) {
        logger.log(null, FQCN, level, tuple.getMessage(), tuple.getArgArray(), tuple.getThrowable());
    }

    /**
     * Checks if trace log level is enabled.
     *
     * @return true if trace logging is enabled, false otherwise.
     */
    @Override
    public boolean isTraceEnabled() {
        return logger.isTraceEnabled();
    }

    /**
     * Logs a TRACE level message if TRACE level logging is enabled.
     *
     * @param msg the message to be logged
     */
    @Override
    public void trace(String msg) {
        if (isTraceEnabled()) {
            log(TRACE_INT, msg);
        }
    }

    /**
     * Logs a formatted message at the TRACE level.
     *
     * @param format The format string
     * @param arg An argument to be formatted within the message
     */
    @Override
    public void trace(String format, Object arg) {
        if (isTraceEnabled()) {
            log(TRACE_INT, org.slf4j.helpers.MessageFormatter.format(format, arg));
        }
    }

    /**
     * Logs a trace message with two arguments if trace logging is enabled.
     *
     * @param format the format string
     * @param argA the first argument
     * @param argB the second argument
     */
    @Override
    public void trace(String format, Object argA, Object argB) {
        if (isTraceEnabled()) {
            log(TRACE_INT, org.slf4j.helpers.MessageFormatter.format(format, argA, argB));
        }
    }

    /**
     * Logs a trace message if trace level logging is enabled.
     *
     * @param format   the format string
     * @param argArray the arguments referenced by the format specifiers in the format string
     */
    @Override
    public void trace(String format, Object... argArray) {
        if (isTraceEnabled()) {
            log(TRACE_INT, org.slf4j.helpers.MessageFormatter.arrayFormat(format, argArray));
        }
    }

    /**
     * Logs a trace message along with an associated throwable if trace logging is enabled.
     *
     * @param msg the trace message to be logged
     * @param t the throwable to be logged along with the message
     */
    @Override
    public void trace(String msg, Throwable t) {
        if (isTraceEnabled()) {
            log(TRACE_INT, msg, t);
        }
    }

    /**
     * Checks if the debug logging level is enabled.
     *
     * @return true if the debug level is enabled, false otherwise.
     */
    @Override
    public boolean isDebugEnabled() {
        return logger.isDebugEnabled();
    }

    /**
     * Logs a debug message if debug level logging is enabled.
     *
     * @param msg The debug message to log
     */
    @Override
    public void debug(String msg) {
        if (isDebugEnabled()) {
            log(DEBUG_INT, msg);
        }
    }

    /**
     * Logs a debug message with a single argument if debug logging is enabled.
     *
     * @param format the format string for the debug message
     * @param arg    the argument applied to the format string
     */
    @Override
    public void debug(String format, Object arg) {
        if (isDebugEnabled()) {
            log(DEBUG_INT, org.slf4j.helpers.MessageFormatter.format(format, arg));
        }
    }

    /**
     * Logs a formatted debug message with two arguments. If debug logging is
     * disabled, this method does nothing.
     *
     * @param format the format string
     * @param argA the first argument referenced by the format specifier
     * @param argB the second argument referenced by the format specifier
     */
    @Override
    public void debug(String format, Object argA, Object argB) {
        if (isDebugEnabled()) {
            log(DEBUG_INT, org.slf4j.helpers.MessageFormatter.format(format, argA, argB));
        }
    }

    /**
     * Logs a debug message using a formatted string and argument array.
     *
     * @param format   the string format for the debug message
     * @param argArray the arguments to be formatted within the message
     */
    @Override
    public void debug(String format, Object... argArray) {
        if (isDebugEnabled()) {
            log(DEBUG_INT, org.slf4j.helpers.MessageFormatter.arrayFormat(format, argArray));
        }
    }

    /**
     * Logs a debug message along with a throwable cause if debug level logging is enabled.
     *
     * @param msg the debug message to log
     * @param t the throwable to log along with the debug message
     */
    @Override
    public void debug(String msg, Throwable t) {
        if (isDebugEnabled()) {
            log(DEBUG_INT, msg, t);
        }
    }

    /**
     * Checks if the INFO level is enabled in the logger.
     *
     * @return true if INFO level logging is enabled, false otherwise.
     */
    @Override
    public boolean isInfoEnabled() {
        return logger.isInfoEnabled();
    }

    /**
     * Logs an informational message if the INFO level is enabled.
     *
     * @param msg the message string to be logged
     */
    @Override
    public void info(String msg) {
        if (isInfoEnabled()) {
            log(INFO_INT, msg);
        }
    }

    /**
     * Logs an info message with the specified format and one argument.
     *
     * @param format the format string
     * @param arg the argument to be formatted and substituted in the format string
     */
    @Override
    public void info(String format, Object arg) {
        if (isInfoEnabled()) {
            log(INFO_INT, org.slf4j.helpers.MessageFormatter.format(format, arg));
        }
    }

    /**
     * Logs an informational message with two arguments using a formatted string.
     *
     * @param format the format string
     * @param argA the first argument to be formatted
     * @param argB the second argument to be formatted
     */
    @Override
    public void info(String format, Object argA, Object argB) {
        if (isInfoEnabled()) {
            log(INFO_INT, org.slf4j.helpers.MessageFormatter.format(format, argA, argB));
        }
    }

    /**
     * Logs a formatted message at the INFO level.
     *
     * @param format   the format string
     * @param argArray the arguments to be substituted in the format string
     */
    @Override
    public void info(String format, Object... argArray) {
        if (isInfoEnabled()) {
            log(INFO_INT, org.slf4j.helpers.MessageFormatter.arrayFormat(format, argArray));
        }
    }

    /**
     * Logs a message with INFO level including the throwable details if INFO level logging is enabled.
     *
     * @param msg the message to be logged
     * @param t the throwable to be logged
     */
    @Override
    public void info(String msg, Throwable t) {
        if (isInfoEnabled()) {
            log(INFO_INT, msg, t);
        }
    }

    /**
     * Checks if the logger is enabled for the WARN level.
     *
     * @return true if the WARN level is enabled, false otherwise.
     */
    @Override
    public boolean isWarnEnabled() {
        return logger.isWarnEnabled();
    }

    /**
     * Logs a warning message if warning level logging is enabled.
     *
     * @param msg the message to be logged
     */
    @Override
    public void warn(String msg) {
        if (isWarnEnabled()) {
            log(WARN_INT, msg);
        }
    }

    /**
     * Logs a warning message with the specified format and argument.
     *
     * @param format the format string
     * @param arg    the argument to be formatted and included in the message
     */
    @Override
    public void warn(String format, Object arg) {
        if (isWarnEnabled()) {
            log(WARN_INT, org.slf4j.helpers.MessageFormatter.format(format, arg));
        }
    }

    /**
     * Logs a warning message with a specified format and an array of arguments.
     *
     * @param format the format string
     * @param argArray the list of arguments to replace placeholders in the format string
     */
    @Override
    public void warn(String format, Object... argArray) {
        if (isWarnEnabled()) {
            log(WARN_INT, org.slf4j.helpers.MessageFormatter.arrayFormat(format, argArray));
        }
    }

    /**
     * Logs a formatted warning message with two arguments if warning is enabled.
     *
     * @param format the format string
     * @param argA the first argument
     * @param argB the second argument
     */
    @Override
    public void warn(String format, Object argA, Object argB) {
        if (isWarnEnabled()) {
            log(WARN_INT, org.slf4j.helpers.MessageFormatter.format(format, argA, argB));
        }
    }

    /**
     * Logs a warning message with an associated throwable if the warn level is enabled.
     *
     * @param msg the warning message to log
     * @param t the throwable to log with the message
     */
    @Override
    public void warn(String msg, Throwable t) {
        if (isWarnEnabled()) {
            log(WARN_INT, msg, t);
        }
    }

    /**
     * Checks whether error logging is currently enabled.
     *
     * @return true if error logging is enabled, false otherwise.
     */
    @Override
    public boolean isErrorEnabled() {
        return logger.isErrorEnabled();
    }

    /**
     * Logs an error message if error logging is enabled.
     *
     * @param msg the error message to be logged
     */
    @Override
    public void error(String msg) {
        if (isErrorEnabled()) {
            log(ERROR_INT, msg);
        }
    }

    /**
     * Logs an error message at the ERROR level.
     *
     * @param format the format string
     * @param arg the argument
     */
    @Override
    public void error(String format, Object arg) {
        if (isErrorEnabled()) {
            log(ERROR_INT, org.slf4j.helpers.MessageFormatter.format(format, arg));
        }
    }

    /**
     * Logs an error message using the specified format and arguments if error logging is enabled.
     *
     * @param format the string format specifying the message
     * @param argA   the first argument to be formatted
     * @param argB   the second argument to be formatted
     */
    @Override
    public void error(String format, Object argA, Object argB) {
        if (isErrorEnabled()) {
            log(ERROR_INT, org.slf4j.helpers.MessageFormatter.format(format, argA, argB));
        }
    }

    /**
     * Logs an error message if error logging is enabled.
     *
     * @param format the format string
     * @param argArray the arguments to be inserted into the format string
     */
    @Override
    public void error(String format, Object... argArray) {
        if (isErrorEnabled()) {
            log(ERROR_INT, org.slf4j.helpers.MessageFormatter.arrayFormat(format, argArray));
        }
    }

    /**
     * Logs an error message with a throwable cause.
     *
     * @param msg the error message to log
     * @param t the throwable cause to log
     */
    @Override
    public void error(String msg, Throwable t) {
        if (isErrorEnabled()) {
            log(ERROR_INT, msg, t);
        }
    }
}
