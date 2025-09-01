package org.ephemq.common.logging;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

public class Slf4JLoggerTest {
    /**
     * An instance of {@link Exception} used for testing purposes in the Slf4JLoggerTest class.
     */
    private static final Exception e = new Exception();

    /**
     * Tests if the trace level logging is enabled.
     * <p>
     * This method creates a mock Logger object, configures its behavior,
     * and verifies that the InternalLogger correctly identifies the trace
     * logging level as being enabled.
     * <p>
     * The test steps include:
     * - Creating a mock Logger instance.
     * - Setting up the mock to return specific values for `getName` and `isTraceEnabled`.
     * - Verifying that the InternalLogger reports trace level as enabled.
     * - Ensuring the mock Logger's methods are called as expected.
     */
    @Test
    public void testIsTraceEnabled() {
        Logger mockLogger = mock(Logger.class);

        when(mockLogger.getName()).thenReturn("foo");
        when(mockLogger.isTraceEnabled()).thenReturn(true);

        InternalLogger logger = new Slf4JLogger(mockLogger);
        assertTrue(logger.isTraceEnabled());

        verify(mockLogger).getName();
        verify(mockLogger).isTraceEnabled();
    }

    /**
     * Tests whether the debug level is enabled for the logger.
     * <p>
     * This method verifies that the `isDebugEnabled` method of a mocked `Logger` instance
     * returns `true` when called, and that the `getName` and `isDebugEnabled` methods
     * of the mock logger are invoked during the test.
     * <p>
     * It instantiates an `InternalLogger` using the `Slf4JLogger` implementation
     * with the mocked `Logger` and asserts that `isDebugEnabled` returns `true`.
     * <p>
     * Mocked methods verified:
     * - `getName` of `Logger` is called.
     * - `isDebugEnabled` of `Logger` is called.
     */
    @Test
    public void testIsDebugEnabled() {
        Logger mockLogger = mock(Logger.class);

        when(mockLogger.getName()).thenReturn("foo");
        when(mockLogger.isDebugEnabled()).thenReturn(true);

        InternalLogger logger = new Slf4JLogger(mockLogger);
        assertTrue(logger.isDebugEnabled());

        verify(mockLogger).getName();
        verify(mockLogger).isDebugEnabled();
    }

    /**
     * Tests the isInfoEnabled method of the InternalLogger class.
     * <p>
     * This test ensures that when the underlying Logger instance's
     * isInfoEnabled method returns true, the InternalLogger instance
     * also reports that info logging is enabled.
     * <p>
     * Two verifications are performed to ensure the mock interactions:
     * - The getName method of the mock Logger has been called.
     * - The isInfoEnabled method of the mock Logger has been called.
     */
    @Test
    public void testIsInfoEnabled() {
        Logger mockLogger = mock(Logger.class);

        when(mockLogger.getName()).thenReturn("foo");
        when(mockLogger.isInfoEnabled()).thenReturn(true);

        InternalLogger logger = new Slf4JLogger(mockLogger);
        assertTrue(logger.isInfoEnabled());

        verify(mockLogger).getName();
        verify(mockLogger).isInfoEnabled();
    }

    /**
     * Tests the isWarnEnabled method of the Slf4JLogger class.
     * This test verifies that the isWarnEnabled method correctly delegates to the underlying SLF4J Logger.
     * The mock Logger is configured to return true for the isWarnEnabled method,
     * and this expectation is verified by invoking the isWarnEnabled method on the Slf4JLogger instance.
     */
    @Test
    public void testIsWarnEnabled() {
        Logger mockLogger = mock(Logger.class);

        when(mockLogger.getName()).thenReturn("foo");
        when(mockLogger.isWarnEnabled()).thenReturn(true);

        InternalLogger logger = new Slf4JLogger(mockLogger);
        assertTrue(logger.isWarnEnabled());

        verify(mockLogger).getName();
        verify(mockLogger).isWarnEnabled();
    }

    /**
     * Tests whether the isErrorEnabled method of the Slf4JLogger correctly delegates
     * to the underlying logger implementation. This test ensures that the Slf4JLogger
     * correctly returns true when the underlying logger indicates that error logging
     * is enabled.
     * <p>
     * Steps:
     *  1. Create a mock Logger instance.
     *  2. Configure the mock Logger to return specific values for getName and isErrorEnabled.
     *  3. Create an instance of InternalLogger using the Slf4JLogger with the mock Logger.
     *  4. Assert that the isErrorEnabled method of the InternalLogger returns true.
     *  5. Verify the interactions with the mock Logger to ensure correct delegation.
     */
    @Test
    public void testIsErrorEnabled() {
        Logger mockLogger = mock(Logger.class);

        when(mockLogger.getName()).thenReturn("foo");
        when(mockLogger.isErrorEnabled()).thenReturn(true);

        InternalLogger logger = new Slf4JLogger(mockLogger);
        assertTrue(logger.isErrorEnabled());

        verify(mockLogger).getName();
        verify(mockLogger).isErrorEnabled();
    }

    /**
     * Tests the trace logging functionality of the Slf4JLogger class.
     * <p>
     * This test verifies that the trace method on the internal logger correctly delegates
     * the trace call to the underlying SLF4J logger. It mocks the Logger class and
     * sets expectations to ensure that the trace message is logged as expected.
     * <p>
     * Preconditions:
     * Mock Logger instance is created and configured to return "foo" as its name.
     * <p>
     * Post conditions:
     * The trace method logs the given message to the underlying SLF4J logger.
     * The getName method of the mocked Logger is called.
     */
    @Test
    public void testTrace() {
        Logger mockLogger = mock(Logger.class);

        when(mockLogger.getName()).thenReturn("foo");

        InternalLogger logger = new Slf4JLogger(mockLogger);
        logger.trace("a");

        verify(mockLogger).getName();
        verify(mockLogger).trace("a");
    }

    /**
     * Tests the trace logging functionality with an exception in the Slf4JLogger class.
     * <p>
     * This test verifies that when a trace message with an exception is logged,
     * the underlying logger's trace method is invoked with the correct parameters.
     * <p>
     * The test sets up a mock Logger, configures it to return a specific name,
     * and then uses this mockLogger to create an instance of Slf4JLogger.
     * <p>
     * The `trace` method of Slf4JLogger is called with a message and an exception,
     * and the test ensures that the mock Logger's `trace` method is called with the same arguments.
     * <p>
     * It verifies two interactions with the mock logger:
     * 1. The logger's name is retrieved via `getName`.
     * 2. The `trace` method is called with the given message and exception.
     */
    @Test
    public void testTraceWithException() {
        Logger mockLogger = mock(Logger.class);

        when(mockLogger.getName()).thenReturn("foo");

        InternalLogger logger = new Slf4JLogger(mockLogger);
        logger.trace("a", e);

        verify(mockLogger).getName();
        verify(mockLogger).trace("a", e);
    }

    /**
     * Tests the debug logging functionality of the {@code Slf4JLogger} class.
     * <p>
     * This method sets up a mock {@code Logger} and assigns it to an {@code InternalLogger}
     * instance of type {@code Slf4JLogger}. It then invokes the {@code debug} method on the
     * {@code Slf4JLogger} with a test message and verifies that the appropriate methods
     * on the mock {@code Logger} were called with the expected arguments.
     */
    @Test
    public void testDebug() {
        Logger mockLogger = mock(Logger.class);

        when(mockLogger.getName()).thenReturn("foo");

        InternalLogger logger = new Slf4JLogger(mockLogger);
        logger.debug("a");

        verify(mockLogger).getName();
        verify(mockLogger).debug("a");
    }

    /**
     * Tests the debug logging functionality of the {@link Slf4JLogger} class
     * when an exception is being logged.
     * <p>
     * This test verifies that the debug method on the underlying mock {@link Logger}
     * is called with the appropriate parameters, including the exception object.
     * <p>
     * Steps performed in this test:
     * 1. Mock a {@link Logger} instance.
     * 2. Set up the mock to return a specific name when {@code getName()} is called.
     * 3. Create an instance of {@link Slf4JLogger} using the mock logger.
     * 4. Invoke the {@code debug} method of {@link Slf4JLogger} with a message and exception.
     * 5. Verify that the mock logger's {@code getName} method and {@code debug} method
     *    were called with the expected arguments.
     */
    @Test
    public void testDebugWithException() {
        Logger mockLogger = mock(Logger.class);

        when(mockLogger.getName()).thenReturn("foo");

        InternalLogger logger = new Slf4JLogger(mockLogger);
        logger.debug("a", e);

        verify(mockLogger).getName();
        verify(mockLogger).debug("a", e);
    }

    /**
     * Tests the info logging functionality of the Slf4JLogger.
     * This method mocks an underlying Logger instance and verifies
     * that the correct logging methods are called.
     */
    @Test
    public void testInfo() {
        Logger mockLogger = mock(Logger.class);

        when(mockLogger.getName()).thenReturn("foo");

        InternalLogger logger = new Slf4JLogger(mockLogger);
        logger.info("a");

        verify(mockLogger).getName();
        verify(mockLogger).info("a");
    }

    /**
     * Tests the info logging functionality when an exception is provided.
     * <p>
     * This test verifies that the `info` method of `Slf4JLogger` correctly
     * logs a message along with an exception. It ensures that the logger's
     * name is fetched and that the `info` method is called with the expected
     * parameters.
     */
    @Test
    public void testInfoWithException() {
        Logger mockLogger = mock(Logger.class);

        when(mockLogger.getName()).thenReturn("foo");

        InternalLogger logger = new Slf4JLogger(mockLogger);
        logger.info("a", e);

        verify(mockLogger).getName();
        verify(mockLogger).info("a", e);
    }

    /**
     * Tests the warn logging functionality of the Slf4JLogger.
     * <p>
     * This test verifies that the warn method of the logger correctly logs a warning
     * message. It mocks a Logger instance, sets up the expected behavior, and checks
     * that the warning message is logged correctly.
     * <p>
     * Steps involved:
     * 1. Create a mock Logger instance.
     * 2. Define the behavior of the mock to return a specific name when getName() is called.
     * 3. Create an instance of InternalLogger using the mock Logger.
     * 4. Invoke the warn method on the InternalLogger instance.
     * 5. Verify that the getName() and warn() methods on the mock Logger were called with the expected arguments.
     */
    @Test
    public void testWarn() {
        Logger mockLogger = mock(Logger.class);

        when(mockLogger.getName()).thenReturn("foo");

        InternalLogger logger = new Slf4JLogger(mockLogger);
        logger.warn("a");

        verify(mockLogger).getName();
        verify(mockLogger).warn("a");
    }

    /**
     * Tests the warn logging with an exception using the Slf4JLogger.
     * This method verifies that the logger's name is retrieved correctly
     * and that the warn level log along with an exception is correctly handled.
     * <p>
     * The mocked Logger is used to simulate logging behavior.
     * <p>
     * Uses the following methods from the Mockito library:
     * - mock(): to create a mock instance of the Logger.
     * - when(): to specify the behavior of the mock instance.
     * - verify(): to verify that specific methods on the mock instance were called.
     * <p>
     * Pre Conditions:
     * - The Logger's name should be set to "foo".
     * <p>
     * Post Conditions:
     * - The Logger's getName() method is called once.
     * - The Logger's warn() method is called with the specified message and exception.
     * <p>
     * Dependencies:
     * - Mockito framework for mocking and verification.
     * - JUnit for the @Test annotation.
     */
    @Test
    public void testWarnWithException() {
        Logger mockLogger = mock(Logger.class);

        when(mockLogger.getName()).thenReturn("foo");

        InternalLogger logger = new Slf4JLogger(mockLogger);
        logger.warn("a", e);

        verify(mockLogger).getName();
        verify(mockLogger).warn("a", e);
    }

    /**
     * Tests the error logging functionality of the Slf4JLogger class.
     * <p>
     * The method verifies that the error message is correctly passed to the underlying logger.
     * It mocks the Logger interface, sets up expectations, and verifies interactions.
     * <p>
     * Pre Conditions:
     * - A mock Logger instance is created and its getName() method is stubbed to return "foo".
     * <p>
     * Post Conditions:
     * - The error message "a" is logged via the Slf4JLogger's error method.
     * - Interactions with the mock Logger are verified.
     */
    @Test
    public void testError() {
        Logger mockLogger = mock(Logger.class);

        when(mockLogger.getName()).thenReturn("foo");

        InternalLogger logger = new Slf4JLogger(mockLogger);
        logger.error("a");

        verify(mockLogger).getName();
        verify(mockLogger).error("a");
    }

    /**
     * Verifies the behavior of the {@code error(String, Throwable)} method in the {@code Slf4JLogger} class
     * when an exception is passed as an argument.
     * <p>
     * The method:
     * - Mocks a Logger instance.
     * - Sets up the mock to return a specific name.
     * - Constructs an {@code InternalLogger} using the mocked Logger.
     * - Calls the {@code error} method on the {@code InternalLogger}.
     * - Verifies that the appropriate methods were called on the mocked Logger with the expected arguments.
     */
    @Test
    public void testErrorWithException() {
        Logger mockLogger = mock(Logger.class);

        when(mockLogger.getName()).thenReturn("foo");

        InternalLogger logger = new Slf4JLogger(mockLogger);
        logger.error("a", e);

        verify(mockLogger).getName();
        verify(mockLogger).error("a", e);
    }
}
