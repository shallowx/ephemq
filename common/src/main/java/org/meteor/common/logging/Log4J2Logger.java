package org.meteor.common.logging;


import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.spi.ExtendedLogger;
import org.apache.logging.log4j.spi.ExtendedLoggerWrapper;

import java.security.AccessController;
import java.security.PrivilegedAction;

class Log4J2Logger extends ExtendedLoggerWrapper implements InternalLogger {

    private static final long serialVersionUID = 5485418394879791397L;
    private static final boolean VARARGS_ONLY;

    static {
        VARARGS_ONLY = AccessController.doPrivileged((PrivilegedAction<Boolean>) () -> {
            try {
                Logger.class.getMethod("debug", String.class, Object.class);
                return false;
            } catch (NoSuchMethodException ignore) {
                return true;
            } catch (SecurityException ignore) {
                return false;
            }
        });
    }

    Log4J2Logger(Logger logger) {
        super((ExtendedLogger) logger, logger.getName(), logger.getMessageFactory());
        if (VARARGS_ONLY) {
            throw new UnsupportedOperationException("Log4J2 version mismatch");
        }
    }

    private static Level toLevel(InternalLogLevel level) {
        return switch (level) {
            case INFO -> Level.INFO;
            case DEBUG -> Level.DEBUG;
            case WARN -> Level.WARN;
            case ERROR -> Level.ERROR;
            case TRACE -> Level.TRACE;
        };
    }

    @Override
    public String name() {
        return getName();
    }

    @Override
    public void trace(Throwable t) {
        log(Level.TRACE, AbstractInternalLogger.EXCEPTION_MESSAGE, t);
    }

    @Override
    public void debug(Throwable t) {
        log(Level.DEBUG, AbstractInternalLogger.EXCEPTION_MESSAGE, t);
    }

    @Override
    public void info(Throwable t) {
        log(Level.INFO, AbstractInternalLogger.EXCEPTION_MESSAGE, t);
    }

    @Override
    public void warn(Throwable t) {
        log(Level.WARN, AbstractInternalLogger.EXCEPTION_MESSAGE, t);
    }

    @Override
    public void error(Throwable t) {
        log(Level.ERROR, AbstractInternalLogger.EXCEPTION_MESSAGE, t);
    }

    @Override
    public boolean isEnabled(InternalLogLevel level) {
        return isEnabled(toLevel(level));
    }

    @Override
    public void log(InternalLogLevel level, String msg) {
        log(toLevel(level), msg);
    }

    @Override
    public void log(InternalLogLevel level, String format, Object arg) {
        log(toLevel(level), format, arg);
    }

    @Override
    public void log(InternalLogLevel level, String format, Object argA, Object argB) {
        log(toLevel(level), format, argA, argB);
    }

    @Override
    public void log(InternalLogLevel level, String format, Object... arguments) {
        log(toLevel(level), format, arguments);
    }

    @Override
    public void log(InternalLogLevel level, String msg, Throwable t) {
        log(toLevel(level), msg, t);
    }

    @Override
    public void log(InternalLogLevel level, Throwable t) {
        log(toLevel(level), AbstractInternalLogger.EXCEPTION_MESSAGE, t);
    }
}
