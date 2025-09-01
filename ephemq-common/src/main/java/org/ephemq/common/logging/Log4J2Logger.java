package org.ephemq.common.logging;

import java.io.Serial;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.spi.ExtendedLogger;
import org.apache.logging.log4j.spi.ExtendedLoggerWrapper;

/**
 * The Log4J2Logger class acts as an adapter to integrate the Log4J2 logging framework
 * into a logging system that uses the InternalLogger interface. It extends the
 * ExtendedLoggerWrapper and implements the InternalLogger interface.
 * <p>
 * This class ensures compatibility with various versions of Log4J2 by checking for the
 * presence of specific logging methods and throws an UnsupportedOperationException
 * if there is a version mismatch.
 * <p>
 * Features include:
 * - Mapping of InternalLogLevel to Log4J2 logging levels.
 * - Support for logging messages with different log levels (TRACE, DEBUG, INFO, WARN, ERROR).
 * - Support for parameterized messages with one or more arguments.
 * - Logging messages along with an associated Throwable for handling exceptions.
 * <p>
 * The log levels supported are INFO, DEBUG, WARN, ERROR, and TRACE.
 */
class Log4J2Logger extends ExtendedLoggerWrapper implements InternalLogger {
    /**
     * A unique identifier for serialization that ensures compatibility between different versions
     * during deserialization.
     */
    @Serial
    private static final long serialVersionUID = 5485418394879791397L;
    /**
     * A flag indicating if only variable argument methods should be used for logging.
     */
    private static boolean VARARGS_ONLY = false;

    static {
        try {
            Logger.class.getMethod("debug", String.class, Object.class);
        } catch (NoSuchMethodException ignore) {
            VARARGS_ONLY = true;
        } catch (SecurityException ignore) {
        }
    }

    /**
     * Constructs a new {@code Log4J2Logger} instance.
     *
     * @param logger the underlying {@code Logger} instance to be wrapped by this logger
     * @throws UnsupportedOperationException if the Log4J2 version is incompatible due to varargs support
     */
    Log4J2Logger(Logger logger) {
        super((ExtendedLogger) logger, logger.getName(), logger.getMessageFactory());
        if (VARARGS_ONLY) {
            throw new UnsupportedOperationException("Log4J2 version mismatch");
        }
    }

    /**
     * Converts an InternalLogLevel to a corresponding Level.
     *
     * @param level the internal log level to be converted
     * @return the corresponding Level instance in the external logging system
     */
    private static Level toLevel(InternalLogLevel level) {
        return switch (level) {
            case INFO -> Level.INFO;
            case DEBUG -> Level.DEBUG;
            case WARN -> Level.WARN;
            case ERROR -> Level.ERROR;
            case TRACE -> Level.TRACE;
        };
    }

    /**
     * Retrieves the name of the logger.
     *
     * @return the name of the logger as a String
     */
    @Override
    public String name() {
        return getName();
    }

    /**
     * Logs a trace level message with the specified throwable.
     *
     * @param t the throwable to be logged at trace level
     */
    @Override
    public void trace(Throwable t) {
        log(Level.TRACE, AbstractInternalLogger.EXCEPTION_MESSAGE, t);
    }

    /**
     * Logs a debug-level message including the given throwable.
     *
     * @param t the throwable to log
     */
    @Override
    public void debug(Throwable t) {
        log(Level.DEBUG, AbstractInternalLogger.EXCEPTION_MESSAGE, t);
    }

    /**
     * Logs a throwable with INFO log level.
     *
     * @param t the throwable to log
     */
    @Override
    public void info(Throwable t) {
        log(Level.INFO, AbstractInternalLogger.EXCEPTION_MESSAGE, t);
    }

    /**
     * Logs a warning message with the specified throwable.
     *
     * @param t the exception to log
     */
    @Override
    public void warn(Throwable t) {
        log(Level.WARN, AbstractInternalLogger.EXCEPTION_MESSAGE, t);
    }

    /**
     * Logs an error message along with the provided Throwable instance.
     *
     * @param t the Throwable instance to be logged
     */
    @Override
    public void error(Throwable t) {
        log(Level.ERROR, AbstractInternalLogger.EXCEPTION_MESSAGE, t);
    }

    /**
     * Checks whether the logger is enabled for a specified log level.
     *
     * @param level The log level to check against.
     * @return true if the logger is enabled for the specified log level, false otherwise.
     */
    @Override
    public boolean isEnabled(InternalLogLevel level) {
        return isEnabled(toLevel(level));
    }

    /**
     * Logs a message at the specified internal log level.
     *
     * @param level the internal log level at which to log the message
     * @param msg the message to log
     */
    @Override
    public void log(InternalLogLevel level, String msg) {
        log(toLevel(level), msg);
    }

    /**
     * Logs a message with the specified log level and argument.
     *
     * @param level the log level at which the message should be logged
     * @param format the format string of the message
     * @param arg the argument referenced by the format specifier
     */
    @Override
    public void log(InternalLogLevel level, String format, Object arg) {
        log(toLevel(level), format, arg);
    }

    /**
     * Logs a formatted message with two arguments at a specified internal log level.
     *
     * @param level  the internal logging level to log the message at; this is mapped to the appropriate external logging level.
     * @param format the message format string, which will be interpolated with the provided arguments.
     * @param argA   the first argument to be included in the formatted message.
     * @param argB   the second argument to be included in the formatted message.
     */
    @Override
    public void log(InternalLogLevel level, String format, Object argA, Object argB) {
        log(toLevel(level), format, argA, argB);
    }

    /**
     * Logs a message with the specified log level, format, and arguments.
     *
     * @param level    the log level to be used for logging the message
     * @param format   the format string for the message
     * @param arguments the arguments referenced by the format specifiers in the format string
     */
    @Override
    public void log(InternalLogLevel level, String format, Object... arguments) {
        log(toLevel(level), format, arguments);
    }

    /**
     * Logs a message with an associated {@link InternalLogLevel} and a {@link Throwable}.
     *
     * @param level the level at which the message should be logged
     * @param msg the message to be logged
     * @param t the {@link Throwable} to be logged
     */
    @Override
    public void log(InternalLogLevel level, String msg, Throwable t) {
        log(toLevel(level), msg, t);
    }

    /**
     * Logs a message at the specified internal log level with the provided throwable instance.
     * This method maps the given {@link InternalLogLevel} to the corresponding {@link Level} and logs
     * an exception message along with the provided throwable.
     *
     * @param level the internal log level at which the message should be logged
     * @param t the throwable instance to be logged
     */
    @Override
    public void log(InternalLogLevel level, Throwable t) {
        log(toLevel(level), AbstractInternalLogger.EXCEPTION_MESSAGE, t);
    }
}
