package org.meteor.common.logging;

import org.meteor.common.util.ObjectUtil;
import org.meteor.common.util.StringUtil;

import java.io.ObjectStreamException;
import java.io.Serial;
import java.io.Serializable;

public abstract class AbstractInternalLogger implements InternalLogger, Serializable {

    static final String EXCEPTION_MESSAGE = "Unexpected exception:";
    @Serial
    private static final long serialVersionUID = -6382972526573193470L;
    private final String name;

    protected AbstractInternalLogger(String name) {
        this.name = ObjectUtil.checkNotNull(name, "name");
    }

    @Override
    public String name() {
        return name;
    }

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

    @Override
    public void trace(Throwable t) {
        trace(EXCEPTION_MESSAGE, t);
    }

    @Override
    public void debug(Throwable t) {
        debug(EXCEPTION_MESSAGE, t);
    }

    @Override
    public void info(Throwable t) {
        info(EXCEPTION_MESSAGE, t);
    }

    @Override
    public void warn(Throwable t) {
        warn(EXCEPTION_MESSAGE, t);
    }

    @Override
    public void error(Throwable t) {
        error(EXCEPTION_MESSAGE, t);
    }

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

    @Serial
    protected Object readResolve() throws ObjectStreamException {
        return InternalLoggerFactory.getLogger(name());
    }

    @Override
    public String toString() {
        return StringUtil.simpleClassName(this) + '(' + name() + ')';
    }
}
