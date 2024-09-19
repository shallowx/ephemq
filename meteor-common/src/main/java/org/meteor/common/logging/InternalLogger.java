package org.meteor.common.logging;

/**
 * The InternalLogger interface provides a standardized contract for logging operations
 * across different logging implementations. It offers methods to log messages at various
 * levels such as TRACE, DEBUG, INFO, WARN, and ERROR.
 * <p>
 * Implementations of this interface should handle the actual logging mechanism and determine
 * if a specific logging level is enabled.
 */
public interface InternalLogger {
    /**
     * Returns the name of the logger.
     *
     * @return the name of the logger
     */
    String name();

    /**
     * Checks if TRACE level logging is enabled.
     *
     * @return {@code true} if TRACE level logging is enabled, {@code false} otherwise
     */
    boolean isTraceEnabled();

    /**
     * Logs a trace level message.
     *
     * @param msg the message to be logged
     */
    void trace(String msg);

    /**
     * Logs a trace message using a formatted string and a single argument.
     *
     * @param format the format string, should follow the {@code String.format} syntax
     * @param arg the argument to be formatted and included in the message
     */
    void trace(String format, Object arg);

    /**
     * Logs a trace message with a formatted string and two arguments.
     *
     * @param format the format string
     * @param argA the first argument to be substituted into the format string
     * @param argB the second argument to be substituted into the format string
     */
    void trace(String format, Object argA, Object argB);

    /**
     * Logs a message at the TRACE level using the specified format string and arguments.
     *
     * @param format the format string
     * @param arguments the arguments referenced by the format string
     */
    void trace(String format, Object... arguments);

    /**
     * Logs a trace message with an associated throwable.
     *
     * @param msg the message to be logged
     * @param t the throwable to be logged
     */
    void trace(String msg, Throwable t);

    /**
     * Logs a throwable at the TRACE level.
     *
     * @param t the throwable to be logged
     */
    void trace(Throwable t);

    /**
     * Checks whether the debug level logging is enabled.
     *
     * @return true if debug level logging is enabled, false otherwise
     */
    boolean isDebugEnabled();

    /**
     * Logs a debug message.
     *
     * @param msg the message to be logged
     */
    void debug(String msg);

    /**
     * Logs a debug message using the specified format and argument.
     *
     * @param format the format string
     * @param arg the argument to be formatted
     */
    void debug(String format, Object arg);

    /**
     * Logs a formatted debug message with the specified arguments.
     *
     * @param format the format string
     * @param argA the first argument to be substituted into the format string
     * @param argB the second argument to be substituted into the format string
     */
    void debug(String format, Object argA, Object argB);

    /**
     * Logs a debug message using the specified format string and arguments.
     *
     * @param format     the format string
     * @param arguments  the arguments referenced by the format string
     */
    void debug(String format, Object... arguments);

    /**
     * Logs a debug message with an associated {@link Throwable}.
     *
     * @param msg the debug message to be logged
     * @param t the throwable associated with the debug message
     */
    void debug(String msg, Throwable t);

    /**
     * Logs a debug message with an associated {@link Throwable}.
     *
     * @param t the {@link Throwable} instance to be logged.
     */
    void debug(Throwable t);

    /**
     * Checks if the INFO logging level is enabled.
     *
     * @return {@code true} if INFO level logging is enabled, {@code false} otherwise
     */
    boolean isInfoEnabled();

    /**
     * Logs an informational message.
     *
     * @param msg the message to be logged
     */
    void info(String msg);

    /**
     * Logs an informational message using the specified format and one argument.
     *
     * @param format the format string containing placeholders for the argument
     * @param arg the argument to be inserted into the format string
     */
    void info(String format, Object arg);

    /**
     * Logs an informational message using the given format and arguments.
     *
     * @param format the format string
     * @param argA the first argument to be substituted into the format string
     * @param argB the second argument to be substituted into the format string
     */
    void info(String format, Object argA, Object argB);

    /**
     * Logs an informational message with a specified format and arguments.
     *
     * @param format the format string
     * @param arguments the arguments referenced by the format string
     */
    void info(String format, Object... arguments);

    /**
     * Logs an informational message along with an optional throwable.
     *
     * @param msg the informational message to be logged
     * @param t the throwable to be logged, can be null
     */
    void info(String msg, Throwable t);

    /**
     * Logs an informational message along with a throwable.
     *
     * @param t the throwable to be logged
     */
    void info(Throwable t);

    /**
     * Checks whether the warn level logging is enabled.
     *
     * @return {@code true} if warn level logging is enabled, {@code false} otherwise.
     */
    boolean isWarnEnabled();

    /**
     * Logs a warning message.
     *
     * @param msg the message to be logged as a warning
     */
    void warn(String msg);

    /**
     * Logs a warning message with a format string and a single argument.
     *
     * @param format the format string
     * @param arg the argument to be formatted
     */
    void warn(String format, Object arg);

    /**
     * Logs a formatted warning message.
     *
     * @param format the format string
     * @param arguments the arguments referenced by the format string
     */
    void warn(String format, Object... arguments);

    /**
     * Logs a formatted warning message with two arguments.
     *
     * @param format the format string
     * @param argA the first argument to be substituted into the format string
     * @param argB the second argument to be substituted into the format string
     */
    void warn(String format, Object argA, Object argB);

    /**
     * Logs a warning message along with a throwable cause.
     *
     * @param msg the warning message to be logged
     * @param t the throwable to be logged along with the warning message
     */
    void warn(String msg, Throwable t);

    /**
     * Issues a warning log message with an exception.
     *
     * @param t the throwable to be logged as a warning
     */
    void warn(Throwable t);

    /**
     * Checks if error logging is enabled.
     *
     * @return true if error logging is enabled, false otherwise.
     */
    boolean isErrorEnabled();

    /**
     * Logs an error message.
     *
     * @param msg The error message to be logged.
     */
    void error(String msg);

    /**
     * Logs an error message with the specified format and argument.
     *
     * @param format the format string
     * @param arg the argument to be formatted
     */
    void error(String format, Object arg);

    /**
     * Logs a formatted error message using the specified arguments.
     *
     * @param format the format string to be used in the error message
     * @param argA the first argument to be substituted into the format string
     * @param argB the second argument to be substituted into the format string
     */
    void error(String format, Object argA, Object argB);

    /**
     * Logs an error message using a format string and corresponding arguments.
     *
     * @param format the format string
     * @param arguments the arguments referenced by the format string
     */
    void error(String format, Object... arguments);

    /**
     * Logs an error message with an associated Throwable.
     *
     * @param msg The error message to be logged.
     * @param t The Throwable to log along with the error message.
     */
    void error(String msg, Throwable t);

    /**
     * Logs an error message with an associated Throwable.
     *
     * @param t The Throwable to log along with the error message.
     */
    void error(Throwable t);

    /**
     * Checks whether logging is enabled for the specified log level.
     *
     * @param level The log level to check for enabling.
     * @return true if logging is enabled for the specified level, false otherwise.
     */
    boolean isEnabled(InternalLogLevel level);

    /**
     * Logs a message at the specified log level.
     *
     * @param level The log level at which the message should be logged.
     * @param msg The message to be logged.
     */
    void log(InternalLogLevel level, String msg);

    /**
     * Logs a message at the specified log level, replacing the format specifier in the message
     * with the provided argument.
     *
     * @param level the level of the log message
     * @param format the message format string
     * @param arg the argument to replace the format specifier in the format string
     */
    void log(InternalLogLevel level, String format, Object arg);

    /**
     * Logs a message with the specified log level, format, and additional arguments.
     *
     * @param level  the log level at which the message should be logged
     * @param format the format string for the log message
     * @param argA   the first argument referenced by the format specifier
     * @param argB   the second argument referenced by the format specifier
     */
    void log(InternalLogLevel level, String format, Object argA, Object argB);

    /**
     * Logs a message with the specified log level, format, and arguments.
     *
     * @param level the log level for the message
     * @param format the format string for the message
     * @param arguments the arguments to be formatted into the message
     */
    void log(InternalLogLevel level, String format, Object... arguments);

    /**
     * Logs a message with a specific log level and throwable.
     *
     * @param level the log level at which the message should be logged
     * @param msg the message to be logged
     * @param t the throwable to be logged along with the message
     */
    void log(InternalLogLevel level, String msg, Throwable t);

    /**
     * Logs a message with an associated {@link Throwable} at the specified {@link InternalLogLevel}.
     *
     * @param level the level at which the message should be logged
     * @param t the {@link Throwable} to be logged
     */
    void log(InternalLogLevel level, Throwable t);
}
