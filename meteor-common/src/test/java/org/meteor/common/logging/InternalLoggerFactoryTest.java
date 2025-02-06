package org.meteor.common.logging;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class InternalLoggerFactoryTest {
    /**
     * A predefined static exception instance used for testing purposes.
     */
    private static final Exception e = new Exception();
    /**
     * Holds a reference to the previous logger factory used in tests.
     * This is used to restore the original logging configuration after
     * test cases have been executed.
     */
    private InternalLoggerFactory oldLoggerFactory;
    /**
     * A mock implementation of the InternalLogger used for testing purposes
     * within the InternalLoggerFactoryTest suite.
     * <p>
     * This logger is used to simulate logging behavior in a controlled environment
     * to verify the logging-related functionality of methods in various tests.
     */
    private InternalLogger mockLogger;

    /**
     * Initializes the test environment before each test is executed.
     * <p>
     * This method performs the following actions:
     * - Stores the current default logger factory.
     * - Mocks a new InternalLoggerFactory and InternalLogger.
     * - Sets the mocked InternalLoggerFactory as the default factory.
     *
     * This setup ensures that the tests run with a controlled and predictable logging environment.
     */
    @BeforeEach
    public void init() {
        oldLoggerFactory = InternalLoggerFactory.getDefaultFactory();

        final InternalLoggerFactory mockFactory = mock(InternalLoggerFactory.class);
        mockLogger = mock(InternalLogger.class);
        when(mockFactory.newLogger("mock")).thenReturn(mockLogger);
        InternalLoggerFactory.setDefaultFactory(mockFactory);
    }

    /**
     * Cleans up the test environment after each test execution.
     * <p>
     * This method is annotated with @AfterEach, which means it is executed after each test method in the
     * `InternalLoggerFactoryTest` class. It performs cleanup activities to ensure that mock objects and
     * state changes do not affect subsequent tests.
     * <p>
     * Specifically, this method:
     * 1. Resets the `mockLogger` to clear any interactions or state.
     * 2. Restores the `InternalLoggerFactory`'s default factory to its prior state using `oldLoggerFactory`.
     */
    @AfterEach
    public void destroy() {
        reset(mockLogger);
        InternalLoggerFactory.setDefaultFactory(oldLoggerFactory);
    }

    /**
     * Test that verifies that setting the default factory of {@link InternalLoggerFactory} to null
     * triggers a {@link NullPointerException}.
     * <p>
     * This method calls {@link InternalLoggerFactory#setDefaultFactory(InternalLoggerFactory)} with a null argument
     * and expects a {@link NullPointerException} to be thrown.
     */
    @Test
    public void shouldNotAllowNullDefaultFactory() {
        assertThrows(NullPointerException.class, new Executable() {
            @Override
            public void execute() {
                InternalLoggerFactory.setDefaultFactory(null);
            }
        });
    }

    /**
     * Tests the functionality of retrieving loggers via the {@link InternalLoggerFactory}.
     * This method asserts that multiple loggers can be retrieved using both string-based and class-based identifiers.
     * It also ensures that the loggers retrieved for different identifiers are different instances.
     * <ul>
     *   <li>Sets the default logger factory to the old logger factory.</li>
     *   <li>Retrieves a logger using a string identifier.</li>
     *   <li>Retrieves a logger using the class of a string identifier.</li>
     *   <li>Verifies that both loggers are not null.</li>
     *   <li>Verifies that the two loggers are not the same instance.</li>
     * </ul>
     *
     * @throws AssertionError if any of the assertions fail.
     * @see InternalLoggerFactory
     */
    @Test
    public void shouldgetLogger() {
        InternalLoggerFactory.setDefaultFactory(oldLoggerFactory);

        String helloWorld = "Hello, world!";

        InternalLogger one = InternalLoggerFactory.getLogger("helloWorld");
        InternalLogger two = InternalLoggerFactory.getLogger(helloWorld.getClass());

        assertNotNull(one);
        assertNotNull(two);
        assertNotSame(one, two);
    }

    /**
     * Test to verify that the `isTraceEnabled` method of the `InternalLogger` returns the expected value.
     * This method sets up the mock logger to return `true` when `isTraceEnabled` is called and
     * verifies that the `InternalLogger` behaves accordingly by asserting that `isTraceEnabled` is `true`.
     * It also ensures that the `isTraceEnabled` method was called on the mock logger.
     */
    @Test
    public void testIsTraceEnabled() {
        when(mockLogger.isTraceEnabled()).thenReturn(true);

        InternalLogger logger = InternalLoggerFactory.getLogger("mock");
        assertTrue(logger.isTraceEnabled());
        verify(mockLogger).isTraceEnabled();
    }

    /**
     * Tests that the debug level logging is enabled.
     * <p>
     * This method mocks a logger and sets it to return true when checking
     * if debug level logging is enabled. It then creates an InternalLogger
     * instance using the mocked logger and asserts that the debug level
     * logging is indeed enabled.
     * <p>
     * The method verifies that the isDebugEnabled() method was called on
     * the mocked logger.
     */
    @Test
    public void testIsDebugEnabled() {
        when(mockLogger.isDebugEnabled()).thenReturn(true);

        InternalLogger logger = InternalLoggerFactory.getLogger("mock");
        assertTrue(logger.isDebugEnabled());
        verify(mockLogger).isDebugEnabled();
    }

    /**
     * Tests the isInfoEnabled method of InternalLogger.
     * <p>
     * This test ensures that the InternalLogger correctly indicates
     * if the INFO logging level is enabled by delegating to the mock logger.
     * <p>
     * Mocks the isInfoEnabled method to return true and verifies
     * that the method on the mock logger was called.
     */
    @Test
    public void testIsInfoEnabled() {
        when(mockLogger.isInfoEnabled()).thenReturn(true);

        InternalLogger logger = InternalLoggerFactory.getLogger("mock");
        assertTrue(logger.isInfoEnabled());
        verify(mockLogger).isInfoEnabled();
    }

    /**
     * Tests if the warn level logging is enabled for the logger.
     * <p>
     * This method sets up a mock logger to return true when the
     * {@code isWarnEnabled} method is called. It uses the
     * {@code InternalLoggerFactory} to get a logger instance and
     * asserts that the warn level logging is enabled.
     */
    @Test
    public void testIsWarnEnabled() {
        when(mockLogger.isWarnEnabled()).thenReturn(true);

        InternalLogger logger = InternalLoggerFactory.getLogger("mock");
        assertTrue(logger.isWarnEnabled());
        verify(mockLogger).isWarnEnabled();
    }

    /**
     * Tests if the error logging level is enabled.
     * <p>
     * This test mocks the behavior of the logger to return true for the isErrorEnabled() method.
     * It then asserts that the InternalLogger instance's isErrorEnabled() method also returns true.
     * Finally, it verifies that the mocked isErrorEnabled() method was called.
     */
    @Test
    public void testIsErrorEnabled() {
        when(mockLogger.isErrorEnabled()).thenReturn(true);

        InternalLogger logger = InternalLoggerFactory.getLogger("mock");
        assertTrue(logger.isErrorEnabled());
        verify(mockLogger).isErrorEnabled();
    }

    /**
     * Tests that the trace method in the InternalLogger logs the correct message.
     * <p>
     * In this test, a logger is retrieved using the InternalLoggerFactory, and
     * a trace message is logged. The test verifies that the mock logger receives
     * the trace method call with the expected message.
     */
    @Test
    public void testTrace() {
        final InternalLogger logger = InternalLoggerFactory.getLogger("mock");
        logger.trace("a");
        verify(mockLogger).trace("a");
    }

    /**
     * Tests logging a trace message with an accompanying exception using the InternalLogger.
     * This method verifies that the trace method is called with the correct parameters when an exception is provided.
     * <p>
     * The logger instance is obtained from the InternalLoggerFactory with the logger name "mock".
     * The test ensures that the trace method of the mockLogger is invoked with the message "a" and the exception object e.
     * <p>
     * It makes use of the JUnit annotation @Test to mark this method as a test case.
     */
    @Test
    public void testTraceWithException() {
        final InternalLogger logger = InternalLoggerFactory.getLogger("mock");
        logger.trace("a", e);
        verify(mockLogger).trace("a", e);
    }

    /**
     * Tests the debug logging functionality.
     * This method verifies that the debug message "a"
     * is correctly passed to the debug logger.
     */
    @Test
    public void testDebug() {
        final InternalLogger logger = InternalLoggerFactory.getLogger("mock");
        logger.debug("a");
        verify(mockLogger).debug("a");
    }

    /**
     * Tests the debug logging functionality with an exception.
     * <p>
     * This method verifies that the {@link InternalLogger#debug(String, Throwable)} method is called
     * with the correct message and exception arguments.
     * <p>
     * The {@link InternalLogger} instance used for testing is obtained from the
     * {@link InternalLoggerFactory} using the logger name "mock". The method checks
     * that the mocked logger's debug method is invoked as expected.
     */
    @Test
    public void testDebugWithException() {
        final InternalLogger logger = InternalLoggerFactory.getLogger("mock");
        logger.debug("a", e);
        verify(mockLogger).debug("a", e);
    }

    /**
     * Validates that the `info` logging method of the `InternalLogger` is called correctly.
     * <p>
     * This test ensures that when the `info` method is invoked on an `InternalLogger` instance,
     * the appropriate logging action is performed on the mock logger.
     */
    @Test
    public void testInfo() {
        final InternalLogger logger = InternalLoggerFactory.getLogger("mock");
        logger.info("a");
        verify(mockLogger).info("a");
    }

    /**
     * Tests the info logging functionality when an exception is provided.
     * <p>
     * This test ensures that the logger correctly logs an info message along with an exception.
     * <p>
     * Steps:
     * 1. Retrieves an instance of InternalLogger.
     * 2. Invokes the logger's info method with a message and an exception.
     * 3. Verifies that the mock logger's info method was called with the same message and exception.
     */
    @Test
    public void testInfoWithException() {
        final InternalLogger logger = InternalLoggerFactory.getLogger("mock");
        logger.info("a", e);
        verify(mockLogger).info("a", e);
    }

    /**
     * Tests the warn logging functionality of the InternalLogger class.
     * <p>
     * This method verifies that a warning message is correctly logged using the InternalLogger.
     * It uses a mock logger to ensure that the warn method is called with the expected message.
     */
    @Test
    public void testWarn() {
        final InternalLogger logger = InternalLoggerFactory.getLogger("mock");
        logger.warn("a");
        verify(mockLogger).warn("a");
    }

    /**
     * Tests the `warn` method of the `InternalLogger` class with an exception.
     * <p>
     * This test verifies that the `warn` method is correctly called with the specified
     * message and exception on the logger.
     */
    @Test
    public void testWarnWithException() {
        final InternalLogger logger = InternalLoggerFactory.getLogger("mock");
        logger.warn("a", e);
        verify(mockLogger).warn("a", e);
    }

    /**
     * Tests the logging of an error message using the InternalLogger.
     * <p>
     * This test verifies that when the error method of the InternalLogger is called
     * with a specific message, the corresponding method of the mockLogger is also called
     * with the same message.
     */
    @Test
    public void testError() {
        final InternalLogger logger = InternalLoggerFactory.getLogger("mock");
        logger.error("a");
        verify(mockLogger).error("a");
    }

    /**
     * Tests the error logging functionality when an exception is provided.
     * This method ensures that the error method in the logger correctly logs the error message
     * and exception.
     */
    @Test
    public void testErrorWithException() {
        final InternalLogger logger = InternalLoggerFactory.getLogger("mock");
        logger.error("a", e);
        verify(mockLogger).error("a", e);
    }
}
