package org.meteor.common.logging;

import io.netty.util.internal.logging.InternalLogLevel;
import io.netty.util.internal.logging.InternalLogger;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public abstract class AbstractInternalLoggerTest<T> {
    /**
     * A map used to store the results of the logging tests.
     * The keys in the map are strings representing different result attributes.
     * The values in the map are the corresponding values for these attributes.
     * This map is utilized and modified across various test methods to assert
     * the correctness of logging operations at different log levels.
     */
    protected final Map<String, Object> result = new HashMap<String, Object>();
    /**
     * The name of the logger being used in the tests.
     */
    protected String loggerName = "foo";
    /**
     * Holds a mock instance of the logger used for testing the internal logging functionalities.
     * It is used in the process of asserting various log levels and log messages during the test cases.
     */
    protected T mockLog;
    /**
     * The logger instance used for internal logging within testing scenarios.
     * This logger is protected and utilizes Netty's InternalLogger for logging purposes.
     * <p>
     * Containing class: AbstractInternalLoggerTest
     * Main purpose: To facilitate and capture logging during unit tests, ensuring that
     * logging behavior conforms to expectations at various log levels and formats.
     * <p>
     * This logger is utilized primarily within the context of methods that test
     * different logging levels and formats, validating both enabled and disabled
     * log scenarios.
     */
    protected io.netty.util.internal.logging.InternalLogger logger;

    /**
     * Retrieves a value from the result map using the provided key.
     *
     * @param <V> The type of the value to be returned.
     * @param key The key for which the value needs to be retrieved.
     * @return The value associated with the given key.
     */
    @SuppressWarnings("unchecked")
    protected <V> V getResult(String key) {
        return (V) result.get(key);
    }

    /**
     * Tests if the logger name is correctly set.
     * <p>
     * This test validates that the name of the logger instance matches the expected loggerName.
     * It uses the assertEquals method from the JUnit framework to compare the expected and actual logger names.
     */
    @Test
    public void testName() {
        assertEquals(loggerName, logger.name());
    }

    /**
     * Tests logging functionality for all defined log levels (TRACE, DEBUG, INFO, WARN, ERROR).
     * This method checks the behavior of logging at each level, ensuring that the appropriate
     * log methods are called and function as expected.
     *
     * @throws Exception if an error occurs during the test.
     */
    @Test
    public void testAllLevel() throws Exception {
        testLevel(io.netty.util.internal.logging.InternalLogLevel.TRACE);
        testLevel(io.netty.util.internal.logging.InternalLogLevel.DEBUG);
        testLevel(io.netty.util.internal.logging.InternalLogLevel.INFO);
        testLevel(io.netty.util.internal.logging.InternalLogLevel.WARN);
        testLevel(io.netty.util.internal.logging.InternalLogLevel.ERROR);
    }

    /**
     * Tests the logging functionality for various levels.
     *
     * @param level The logging level to test, which is an instance of {@link InternalLogLevel}.
     * @throws Exception if any reflection or logging error occurs during the test.
     */
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
                new Object[]{msg, msg, msg});
        assertTrue(result.isEmpty());

        // test xx(format, ...arguments), the last argument is Throwable
        clazz.getMethod(logMethod, String.class, Object[].class).invoke(logger, format3,
                new Object[]{msg, msg, msg, ex});
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
                new Object[]{msg, msg, msg});
        assertResult(level, format3, null, msg, msg, msg);

        // test xx(format, ...arguments), the last argument is Throwable
        result.clear();
        clazz.getMethod(logMethod, String.class, Object[].class).invoke(logger, format3,
                new Object[]{msg, msg, msg, ex});
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

    /**
     * Asserts that the logging result is not empty when a log at the provided level,
     * format, and arguments is made.
     *
     * @param level the log level at which the message was logged
     * @param format the format of the log message
     * @param t the throwable associated with the log message, if any
     * @param args the arguments used in the formatted log message
     */
    protected void assertResult(io.netty.util.internal.logging.InternalLogLevel level, String format, Throwable t, Object... args) {
        assertFalse(result.isEmpty());
    }

    /**
     * Sets the log level enablement for a given internal log level.
     *
     * @param level The internal log level to enable or disable.
     * @param enable A boolean indicating whether to enable or disable the specified log level.
     * @throws Exception if any error occurs while setting the log level enablement.
     */
    protected abstract void setLevelEnable(InternalLogLevel level, boolean enable) throws Exception;
}
