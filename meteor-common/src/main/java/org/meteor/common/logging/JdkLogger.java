package org.meteor.common.logging;

import java.io.Serial;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * JdkLogger is an implementation of AbstractInternalLogger that utilizes the
 * java.util.logging.Logger for logging messages. It supports various logging levels
 * such as TRACE, DEBUG, INFO, WARN, and ERROR, allowing messages to be formatted
 * and logged with different severity.
 */
class JdkLogger extends AbstractInternalLogger {
    /**
     * Holds the fully qualified name of the current class.
     */
    static final String SELF = JdkLogger.class.getName();
    /**
     * A constant representing the name of the superclass, which is used for logging purposes.
     * This is derived from the class name of {@code AbstractInternalLogger}.
     */
    static final String SUPER = AbstractInternalLogger.class.getName();
    /**
     * A unique identifier for this Serializable class, ensuring compatibility
     * between different versions of the class.
     */
    @Serial
    private static final long serialVersionUID = -1767272577989225979L;
    /**
     * Logger used for logging messages within the JdkLogger class.
     * <p>
     * This logger is responsible for handling various logging levels,
     * such as trace, debug, info, warn, and error. It is an instance
     * of the Logger class and is utilized to record log entries.
     * <p>
     * The logger is marked as `final` indicating it cannot be reassigned,
     * and `transient`, implying it should not be serialized.
     */
    final transient Logger logger;

    /**
     * Constructs a JdkLogger instance using the specified java.util.logging.Logger.
     *
     * @param logger The java.util.logging.Logger instance to be used by this logger.
     */
    JdkLogger(Logger logger) {
        super(logger.getName());
        this.logger = logger;
    }

    /**
     * Fills the LogRecord object with the source class name and source method name
     * of the caller by analyzing the current stack trace.
     *
     * @param record the LogRecord object to be filled with caller data
     */
    private static void fillCallerData(LogRecord record) {
        StackTraceElement[] steArray = new Throwable().getStackTrace();

        int selfIndex = -1;
        for (int i = 0; i < steArray.length; i++) {
            final String className = steArray[i].getClassName();
            if (className.equals(SELF) || className.equals(SUPER)) {
                selfIndex = i;
                break;
            }
        }

        int found = -1;
        for (int i = selfIndex + 1; i < steArray.length; i++) {
            final String className = steArray[i].getClassName();
            if (!(className.equals(SELF) || className.equals(SUPER))) {
                found = i;
                break;
            }
        }

        if (found != -1) {
            StackTraceElement ste = steArray[found];
            record.setSourceClassName(ste.getClassName());
            record.setSourceMethodName(ste.getMethodName());
        }
    }

    /**
     * Checks if trace level logging is enabled.
     *
     * @return true if trace level logging is enabled, false otherwise.
     */
    @Override
    public boolean isTraceEnabled() {
        return logger.isLoggable(Level.FINEST);
    }

    /**
     * Logs a trace message if the logger's level is set to FINEST.
     *
     * @param msg The message to be logged at the trace level.
     */
    @Override
    public void trace(String msg) {
        if (logger.isLoggable(Level.FINEST)) {
            log(Level.FINEST, msg, null);
        }
    }

    /**
     * Logs a trace message with a single argument.
     * This method will format the message according to the specified format and argument.
     *
     * @param format the format string
     * @param arg the argument to be used in the format
     */
    @Override
    public void trace(String format, Object arg) {
        if (logger.isLoggable(Level.FINEST)) {
            FormattingTuple ft = MessageFormatter.format(format, arg);
            log(Level.FINEST, ft.getMessage(), ft.getThrowable());
        }
    }

    /**
     * Logs a trace message at the FINEST level using the specified format and arguments.
     * If the logger is not enabled for the FINEST level, the message will not be logged.
     *
     * @param format the format string
     * @param argA the first argument for formatting
     * @param argB the second argument for formatting
     */
    @Override
    public void trace(String format, Object argA, Object argB) {
        if (logger.isLoggable(Level.FINEST)) {
            FormattingTuple ft = MessageFormatter.format(format, argA, argB);
            log(Level.FINEST, ft.getMessage(), ft.getThrowable());
        }
    }

    /**
     * Logs a trace message with optional formatting and arguments.
     * If the logger is enabled for the FINEST level, the message will be formatted
     * and logged along with any associated throwable.
     *
     * @param format the format string
     * @param argArray the arguments referenced by the format specifiers in the format string
     */
    @Override
    public void trace(String format, Object... argArray) {
        if (logger.isLoggable(Level.FINEST)) {
            FormattingTuple ft = MessageFormatter.arrayFormat(format, argArray);
            log(Level.FINEST, ft.getMessage(), ft.getThrowable());
        }
    }

    /**
     * Logs a trace message along with a throwable if the logger is loggable at the FINEST level.
     *
     * @param msg The trace message to be logged.
     * @param t The throwable to be logged with the message.
     */
    @Override
    public void trace(String msg, Throwable t) {
        if (logger.isLoggable(Level.FINEST)) {
            log(Level.FINEST, msg, t);
        }
    }

    /**
     * Checks if debug level logging is enabled.
     *
     * @return true if debug level logging is enabled, false otherwise.
     */
    @Override
    public boolean isDebugEnabled() {
        return logger.isLoggable(Level.FINE);
    }

    /**
     * Logs a debug message if the logger level is set to FINE.
     *
     * @param msg the debug message to be logged
     */
    @Override
    public void debug(String msg) {
        if (logger.isLoggable(Level.FINE)) {
            log(Level.FINE, msg, null);
        }
    }

    /**
     * Logs a debug message using the specified format string and argument, if the logger is enabled for debug-level output.
     * If the logger is enabled for debug-level output, the message is formatted using the specified format string and argument,
     * and then logged along with any associated throwable.
     *
     * @param format the format string of the debug message
     * @param arg the argument to be inserted into the format string
     */
    @Override
    public void debug(String format, Object arg) {
        if (logger.isLoggable(Level.FINE)) {
            FormattingTuple ft = MessageFormatter.format(format, arg);
            log(Level.FINE, ft.getMessage(), ft.getThrowable());
        }
    }

    /**
     * Logs a debug message with the specified format and two arguments.
     * The message will only be logged if the debug level is enabled.
     *
     * @param format the string format for the log message
     * @param argA the first argument to be applied to the string format
     * @param argB the second argument to be applied to the string format
     */
    @Override
    public void debug(String format, Object argA, Object argB) {
        if (logger.isLoggable(Level.FINE)) {
            FormattingTuple ft = MessageFormatter.format(format, argA, argB);
            log(Level.FINE, ft.getMessage(), ft.getThrowable());
        }
    }

    /**
     * Logs a debug message using a format string and arguments.
     * This method checks if the logger is set to the debug level and formats
     * the message with the provided arguments before logging it.
     *
     * @param format the format string
     * @param argArray the arguments referenced by the format string
     */
    @Override
    public void debug(String format, Object... argArray) {
        if (logger.isLoggable(Level.FINE)) {
            FormattingTuple ft = MessageFormatter.arrayFormat(format, argArray);
            log(Level.FINE, ft.getMessage(), ft.getThrowable());
        }
    }

    /**
     * Logs a debug message along with a throwable if the logger is loggable at the FINE level.
     *
     * @param msg the message to log
     * @param t the throwable to log
     */
    @Override
    public void debug(String msg, Throwable t) {
        if (logger.isLoggable(Level.FINE)) {
            log(Level.FINE, msg, t);
        }
    }

    /**
     * Checks whether the INFO level is enabled for logging.
     *
     * @return true if the INFO level is enabled; false otherwise.
     */
    @Override
    public boolean isInfoEnabled() {
        return logger.isLoggable(Level.INFO);
    }

    /**
     * Logs a message at the INFO level.
     *
     * @param msg the message string to be logged
     */
    @Override
    public void info(String msg) {
        if (logger.isLoggable(Level.INFO)) {
            log(Level.INFO, msg, null);
        }
    }

    /**
     * Logs an information message with a single argument. If the specified logging level
     * is loggable, it formats the provided message and argument, and logs it.
     *
     * @param format the format string
     * @param arg the argument for the format string
     */
    @Override
    public void info(String format, Object arg) {
        if (logger.isLoggable(Level.INFO)) {
            FormattingTuple ft = MessageFormatter.format(format, arg);
            log(Level.INFO, ft.getMessage(), ft.getThrowable());
        }
    }

    /**
     * Logs an informational message using a formatted string and two arguments.
     *
     * @param format the format string
     * @param argA the first argument referenced by the format specifier
     * @param argB the second argument referenced by the format specifier
     */
    @Override
    public void info(String format, Object argA, Object argB) {
        if (logger.isLoggable(Level.INFO)) {
            FormattingTuple ft = MessageFormatter.format(format, argA, argB);
            log(Level.INFO, ft.getMessage(), ft.getThrowable());
        }
    }

    /**
     * Logs an informational message with an optional list of arguments using the specified format.
     * The message is only logged if the logger is currently set to log INFO level messages.
     *
     * @param format the format string
     * @param argArray the arguments referenced by the format specifiers in the format string
     */
    @Override
    public void info(String format, Object... argArray) {
        if (logger.isLoggable(Level.INFO)) {
            FormattingTuple ft = MessageFormatter.arrayFormat(format, argArray);
            log(Level.INFO, ft.getMessage(), ft.getThrowable());
        }
    }

    /**
     * Logs an informational message along with an associated throwable if the current log level allows it.
     *
     * @param msg The informational message to be logged.
     * @param t The throwable to be logged along with the message.
     */
    @Override
    public void info(String msg, Throwable t) {
        if (logger.isLoggable(Level.INFO)) {
            log(Level.INFO, msg, t);
        }
    }

    /**
     * Checks if the warn log level is enabled.
     *
     * @return true if warn logging is enabled, false otherwise
     */
    @Override
    public boolean isWarnEnabled() {
        return logger.isLoggable(Level.WARNING);
    }

    /**
     * Logs a warning message if the logger is set to the WARNING level.
     *
     * @param msg the warning message to be logged
     */
    @Override
    public void warn(String msg) {
        if (logger.isLoggable(Level.WARNING)) {
            log(Level.WARNING, msg, null);
        }
    }

    /**
     * Logs a warning message with an optional single argument, using the specified format.
     * <p>
     * If the logging level is set to warning, the message will be formatted with the provided
     * argument and then logged. The {@link MessageFormatter#format} method is used to generate
     * the formatted message.
     *
     * @param format the format string
     * @param arg the argument to format within the format string
     */
    @Override
    public void warn(String format, Object arg) {
        if (logger.isLoggable(Level.WARNING)) {
            FormattingTuple ft = MessageFormatter.format(format, arg);
            log(Level.WARNING, ft.getMessage(), ft.getThrowable());
        }
    }

    /**
     * Logs a formatted warning message with two arguments.
     * If the logger is currently enabled for the WARNING level, it formats
     * the given message with the specified arguments and logs the result.
     *
     * @param format the format string
     * @param argA the first argument to be formatted and substituted in the format string
     * @param argB the second argument to be formatted and substituted in the format string
     */
    @Override
    public void warn(String format, Object argA, Object argB) {
        if (logger.isLoggable(Level.WARNING)) {
            FormattingTuple ft = MessageFormatter.format(format, argA, argB);
            log(Level.WARNING, ft.getMessage(), ft.getThrowable());
        }
    }

    /**
     * Logs a formatted warning message using the provided format string and arguments.
     * If the logger is not enabled for the WARNING level, the message is not logged.
     *
     * @param format the format string
     * @param argArray the arguments to be substituted in the format string
     */
    @Override
    public void warn(String format, Object... argArray) {
        if (logger.isLoggable(Level.WARNING)) {
            FormattingTuple ft = MessageFormatter.arrayFormat(format, argArray);
            log(Level.WARNING, ft.getMessage(), ft.getThrowable());
        }
    }

    /**
     * Logs a warning message along with a throwable.
     *
     * @param msg the warning message to be logged
     * @param t the throwable to be logged
     */
    @Override
    public void warn(String msg, Throwable t) {
        if (logger.isLoggable(Level.WARNING)) {
            log(Level.WARNING, msg, t);
        }
    }

    /**
     * Checks if error logging is enabled.
     *
     * @return true if messages logged at the SEVERE level will be logged, false otherwise.
     */
    @Override
    public boolean isErrorEnabled() {
        return logger.isLoggable(Level.SEVERE);
    }

    /**
     * Logs an error message with severity level SEVERE if the logger is loggable.
     *
     * @param msg the message to be logged
     */
    @Override
    public void error(String msg) {
        if (logger.isLoggable(Level.SEVERE)) {
            log(Level.SEVERE, msg, null);
        }
    }

    /**
     * Logs an error message at the SEVERE level using a format string and a single argument.
     *
     * @param format the format string
     * @param arg    the argument to include in the formatted message
     */
    @Override
    public void error(String format, Object arg) {
        if (logger.isLoggable(Level.SEVERE)) {
            FormattingTuple ft = MessageFormatter.format(format, arg);
            log(Level.SEVERE, ft.getMessage(), ft.getThrowable());
        }
    }

    /**
     * Logs an error message with a specified format and two arguments at the SEVERE level.
     *
     * @param format the format string
     * @param argA the first argument to be included in the formatted message
     * @param argB the second argument to be included in the formatted message
     */
    @Override
    public void error(String format, Object argA, Object argB) {
        if (logger.isLoggable(Level.SEVERE)) {
            FormattingTuple ft = MessageFormatter.format(format, argA, argB);
            log(Level.SEVERE, ft.getMessage(), ft.getThrowable());
        }
    }

    /**
     * Logs an error message using the provided format string and arguments.
     *
     * @param format the string containing the text to be formatted
     * @param arguments the arguments to be inserted into the format string
     */
    @Override
    public void error(String format, Object... arguments) {
        if (logger.isLoggable(Level.SEVERE)) {
            FormattingTuple ft = MessageFormatter.arrayFormat(format, arguments);
            log(Level.SEVERE, ft.getMessage(), ft.getThrowable());
        }
    }

    /**
     * Logs an error message along with a throwable.
     *
     * @param msg the error message to be logged
     * @param t the throwable associated with the error
     */
    @Override
    public void error(String msg, Throwable t) {
        if (logger.isLoggable(Level.SEVERE)) {
            log(Level.SEVERE, msg, t);
        }
    }

    /**
     * Logs a message with the specified level and throwable.
     *
     * @param level the log level
     * @param msg the message to be logged
     * @param t the throwable associated with the log message
     */
    private void log(Level level, String msg, Throwable t) {
        LogRecord record = new LogRecord(level, msg);
        record.setLoggerName(name());
        record.setThrown(t);
        fillCallerData(record);
        logger.log(record);
    }
}
