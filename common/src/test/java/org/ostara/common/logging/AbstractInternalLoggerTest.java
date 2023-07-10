package org.ostara.common.logging;

import io.netty.util.internal.logging.InternalLogLevel;
import io.netty.util.internal.logging.InternalLogger;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public abstract class AbstractInternalLoggerTest<T> {
    protected String loggerName = "foo";
    protected T mockLog;
    protected io.netty.util.internal.logging.InternalLogger logger;
    protected final Map<String, Object> result = new HashMap<String, Object>();

    @SuppressWarnings("unchecked")
    protected <V> V getResult(String key) {
        return (V) result.get(key);
    }

    @Test
    public void testName() {
        assertEquals(loggerName, logger.name());
    }

    @Test
    public void testAllLevel() throws Exception {
        testLevel(io.netty.util.internal.logging.InternalLogLevel.TRACE);
        testLevel(io.netty.util.internal.logging.InternalLogLevel.DEBUG);
        testLevel(io.netty.util.internal.logging.InternalLogLevel.INFO);
        testLevel(io.netty.util.internal.logging.InternalLogLevel.WARN);
        testLevel(io.netty.util.internal.logging.InternalLogLevel.ERROR);
    }

    protected void testLevel(io.netty.util.internal.logging.InternalLogLevel level) throws Exception {
        result.clear();

        String format1 = "a={}", format2 = "a={}, b= {}", format3 = "a={}, b= {}, c= {}";
        String msg = "a test message from Junit";
        Exception ex = new Exception("a test Exception from Junit");

        Class<io.netty.util.internal.logging.InternalLogger> clazz = InternalLogger.class;
        String levelName = level.name(), logMethod = levelName.toLowerCase();
        Method isXXEnabled = clazz
                .getMethod("is" + levelName.charAt(0) + levelName.substring(1).toLowerCase() + "Enabled");

        // when level log is disabled
        setLevelEnable(level, false);
        assertFalse((Boolean) isXXEnabled.invoke(logger));

        // test xx(msg)
        clazz.getMethod(logMethod, String.class).invoke(logger, msg);
        assertTrue(result.isEmpty());

        // test xx(format, arg)
        clazz.getMethod(logMethod, String.class, Object.class).invoke(logger, format1, msg);
        assertTrue(result.isEmpty());

        // test xx(format, argA, argB)
        clazz.getMethod(logMethod, String.class, Object.class, Object.class).invoke(logger, format2, msg, msg);
        assertTrue(result.isEmpty());

        // test xx(format, ...arguments)
        clazz.getMethod(logMethod, String.class, Object[].class).invoke(logger, format3,
                new Object[] { msg, msg, msg });
        assertTrue(result.isEmpty());

        // test xx(format, ...arguments), the last argument is Throwable
        clazz.getMethod(logMethod, String.class, Object[].class).invoke(logger, format3,
                new Object[] { msg, msg, msg, ex });
        assertTrue(result.isEmpty());

        // test xx(msg, Throwable)
        clazz.getMethod(logMethod, String.class, Throwable.class).invoke(logger, msg, ex);
        assertTrue(result.isEmpty());

        // test xx(Throwable)
        clazz.getMethod(logMethod, Throwable.class).invoke(logger, ex);
        assertTrue(result.isEmpty());

        // when level log is enabled
        setLevelEnable(level, true);
        assertTrue((Boolean) isXXEnabled.invoke(logger));

        // test xx(msg)
        result.clear();
        clazz.getMethod(logMethod, String.class).invoke(logger, msg);
        assertResult(level, null, null, msg);

        // test xx(format, arg)
        result.clear();
        clazz.getMethod(logMethod, String.class, Object.class).invoke(logger, format1, msg);
        assertResult(level, format1, null, msg);

        // test xx(format, argA, argB)
        result.clear();
        clazz.getMethod(logMethod, String.class, Object.class, Object.class).invoke(logger, format2, msg, msg);
        assertResult(level, format2, null, msg, msg);

        // test xx(format, ...arguments)
        result.clear();
        clazz.getMethod(logMethod, String.class, Object[].class).invoke(logger, format3,
                new Object[] { msg, msg, msg });
        assertResult(level, format3, null, msg, msg, msg);

        // test xx(format, ...arguments), the last argument is Throwable
        result.clear();
        clazz.getMethod(logMethod, String.class, Object[].class).invoke(logger, format3,
                new Object[] { msg, msg, msg, ex });
        assertResult(level, format3, ex, msg, msg, msg, ex);

        // test xx(msg, Throwable)
        result.clear();
        clazz.getMethod(logMethod, String.class, Throwable.class).invoke(logger, msg, ex);
        assertResult(level, null, ex, msg);

        // test xx(Throwable)
        result.clear();
        clazz.getMethod(logMethod, Throwable.class).invoke(logger, ex);
        assertResult(level, null, ex);
    }

    /** a just default code, you can override to fix {@linkplain #mockLog} */
    protected void assertResult(io.netty.util.internal.logging.InternalLogLevel level, String format, Throwable t, Object... args) {
        assertFalse(result.isEmpty());
    }

    protected abstract void setLevelEnable(InternalLogLevel level, boolean enable) throws Exception;
}
