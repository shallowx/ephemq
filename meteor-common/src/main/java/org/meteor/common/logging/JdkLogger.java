package org.meteor.common.logging;

import java.io.Serial;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

class JdkLogger extends AbstractInternalLogger {
    static final String SELF = JdkLogger.class.getName();
    static final String SUPER = AbstractInternalLogger.class.getName();
    @Serial
    private static final long serialVersionUID = -1767272577989225979L;
    final transient Logger logger;

    JdkLogger(Logger logger) {
        super(logger.getName());
        this.logger = logger;
    }

    @Override
    public boolean isTraceEnabled() {
        return logger.isLoggable(Level.FINEST);
    }

    @Override
    public void trace(String msg) {
        if (logger.isLoggable(Level.FINEST)) {
            log(Level.FINEST, msg, null);
        }
    }

    @Override
    public void trace(String format, Object arg) {
        if (logger.isLoggable(Level.FINEST)) {
            FormattingTuple ft = MessageFormatter.format(format, arg);
            log(Level.FINEST, ft.getMessage(), ft.getThrowable());
        }
    }

    @Override
    public void trace(String format, Object argA, Object argB) {
        if (logger.isLoggable(Level.FINEST)) {
            FormattingTuple ft = MessageFormatter.format(format, argA, argB);
            log(Level.FINEST, ft.getMessage(), ft.getThrowable());
        }
    }

    @Override
    public void trace(String format, Object... argArray) {
        if (logger.isLoggable(Level.FINEST)) {
            FormattingTuple ft = MessageFormatter.arrayFormat(format, argArray);
            log(Level.FINEST, ft.getMessage(), ft.getThrowable());
        }
    }

    @Override
    public void trace(String msg, Throwable t) {
        if (logger.isLoggable(Level.FINEST)) {
            log(Level.FINEST, msg, t);
        }
    }

    @Override
    public boolean isDebugEnabled() {
        return logger.isLoggable(Level.FINE);
    }

    @Override
    public void debug(String msg) {
        if (logger.isLoggable(Level.FINE)) {
            log(Level.FINE, msg, null);
        }
    }

    @Override
    public void debug(String format, Object arg) {
        if (logger.isLoggable(Level.FINE)) {
            FormattingTuple ft = MessageFormatter.format(format, arg);
            log(Level.FINE, ft.getMessage(), ft.getThrowable());
        }
    }

    @Override
    public void debug(String format, Object argA, Object argB) {
        if (logger.isLoggable(Level.FINE)) {
            FormattingTuple ft = MessageFormatter.format(format, argA, argB);
            log(Level.FINE, ft.getMessage(), ft.getThrowable());
        }
    }

    @Override
    public void debug(String format, Object... argArray) {
        if (logger.isLoggable(Level.FINE)) {
            FormattingTuple ft = MessageFormatter.arrayFormat(format, argArray);
            log(Level.FINE, ft.getMessage(), ft.getThrowable());
        }
    }

    @Override
    public void debug(String msg, Throwable t) {
        if (logger.isLoggable(Level.FINE)) {
            log(Level.FINE, msg, t);
        }
    }

    @Override
    public boolean isInfoEnabled() {
        return logger.isLoggable(Level.INFO);
    }

    @Override
    public void info(String msg) {
        if (logger.isLoggable(Level.INFO)) {
            log(Level.INFO, msg, null);
        }
    }

    @Override
    public void info(String format, Object arg) {
        if (logger.isLoggable(Level.INFO)) {
            FormattingTuple ft = MessageFormatter.format(format, arg);
            log(Level.INFO, ft.getMessage(), ft.getThrowable());
        }
    }

    @Override
    public void info(String format, Object argA, Object argB) {
        if (logger.isLoggable(Level.INFO)) {
            FormattingTuple ft = MessageFormatter.format(format, argA, argB);
            log(Level.INFO, ft.getMessage(), ft.getThrowable());
        }
    }

    @Override
    public void info(String format, Object... argArray) {
        if (logger.isLoggable(Level.INFO)) {
            FormattingTuple ft = MessageFormatter.arrayFormat(format, argArray);
            log(Level.INFO, ft.getMessage(), ft.getThrowable());
        }
    }

    @Override
    public void info(String msg, Throwable t) {
        if (logger.isLoggable(Level.INFO)) {
            log(Level.INFO, msg, t);
        }
    }

    @Override
    public boolean isWarnEnabled() {
        return logger.isLoggable(Level.WARNING);
    }

    @Override
    public void warn(String msg) {
        if (logger.isLoggable(Level.WARNING)) {
            log(Level.WARNING, msg, null);
        }
    }

    @Override
    public void warn(String format, Object arg) {
        if (logger.isLoggable(Level.WARNING)) {
            FormattingTuple ft = MessageFormatter.format(format, arg);
            log(Level.WARNING, ft.getMessage(), ft.getThrowable());
        }
    }

    @Override
    public void warn(String format, Object argA, Object argB) {
        if (logger.isLoggable(Level.WARNING)) {
            FormattingTuple ft = MessageFormatter.format(format, argA, argB);
            log(Level.WARNING, ft.getMessage(), ft.getThrowable());
        }
    }

    @Override
    public void warn(String format, Object... argArray) {
        if (logger.isLoggable(Level.WARNING)) {
            FormattingTuple ft = MessageFormatter.arrayFormat(format, argArray);
            log(Level.WARNING, ft.getMessage(), ft.getThrowable());
        }
    }

    @Override
    public void warn(String msg, Throwable t) {
        if (logger.isLoggable(Level.WARNING)) {
            log(Level.WARNING, msg, t);
        }
    }

    @Override
    public boolean isErrorEnabled() {
        return logger.isLoggable(Level.SEVERE);
    }

    @Override
    public void error(String msg) {
        if (logger.isLoggable(Level.SEVERE)) {
            log(Level.SEVERE, msg, null);
        }
    }

    @Override
    public void error(String format, Object arg) {
        if (logger.isLoggable(Level.SEVERE)) {
            FormattingTuple ft = MessageFormatter.format(format, arg);
            log(Level.SEVERE, ft.getMessage(), ft.getThrowable());
        }
    }

    @Override
    public void error(String format, Object argA, Object argB) {
        if (logger.isLoggable(Level.SEVERE)) {
            FormattingTuple ft = MessageFormatter.format(format, argA, argB);
            log(Level.SEVERE, ft.getMessage(), ft.getThrowable());
        }
    }

    @Override
    public void error(String format, Object... arguments) {
        if (logger.isLoggable(Level.SEVERE)) {
            FormattingTuple ft = MessageFormatter.arrayFormat(format, arguments);
            log(Level.SEVERE, ft.getMessage(), ft.getThrowable());
        }
    }

    @Override
    public void error(String msg, Throwable t) {
        if (logger.isLoggable(Level.SEVERE)) {
            log(Level.SEVERE, msg, t);
        }
    }
    
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

    private void log(Level level, String msg, Throwable t) {
        LogRecord record = new LogRecord(level, msg);
        record.setLoggerName(name());
        record.setThrown(t);
        fillCallerData(record);
        logger.log(record);
    }
}
