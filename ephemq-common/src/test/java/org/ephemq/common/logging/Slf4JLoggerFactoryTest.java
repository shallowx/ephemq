package org.ephemq.common.logging;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.slf4j.Logger;
import org.slf4j.spi.LocationAwareLogger;

import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class Slf4JLoggerFactoryTest {

    /**
     * Tests the creation of an SLF4J logger through the Slf4JLoggerFactory.
     * <p>
     * This method verifies that the Slf4JLoggerFactory correctly creates an instance of
     * InternalLogger, ensuring the logger is either an instance of Slf4JLogger or
     * LocationAwareSlf4JLogger. Additionally, it checks the name of the logger to ensure
     * it matches the input name "foo".
     */
    @Test
    public void testCreation() {
        Slf4JLoggerFactory factory = new Slf4JLoggerFactory(true);
        InternalLogger logger = factory.newLogger("foo");
        assertTrue(logger instanceof Slf4JLogger || logger instanceof LocationAwareSlf4JLogger);
        assertEquals("foo", logger.name());
    }

    /**
     * Tests the creation and functionality of the logger instance within the Slf4JLoggerFactory.
     * Specifically, it verifies that the factory correctly wraps a given logger and that the
     * resulting InternalLogger instance is of type Slf4JLogger. Furthermore, it checks that the
     * name of the logger is accurately retrieved.
     * <p>
     * Test actions:
     * 1. Mocks a Logger instance and sets its behavior for getName() method.
     * 2. Wraps the mocked Logger using Slf4JLoggerFactory.
     * 3. Asserts that the wrapped Logger instance is of type Slf4JLogger.
     * 4. Asserts that the name of the wrapped logger is correctly set.
     */
    @Test
    public void testCreationLogger() {
        Logger logger = mock(Logger.class);
        when(logger.getName()).thenReturn("testlogger");
        InternalLogger internalLogger = Slf4JLoggerFactory.wrapLogger(logger);
        assertInstanceOf(Slf4JLogger.class, internalLogger);
        assertEquals("testlogger", internalLogger.name());
    }

    /**
     * Tests the creation of a location-aware logger.
     * This method verifies that a mock instance of LocationAwareLogger can be successfully wrapped
     * by Slf4JLoggerFactory and that the resulting internal logger is of type LocationAwareSlf4JLogger.
     * The test also ensures that the logger name is correctly set to "testlogger".
     */
    @Test
    public void testCreationLocationAwareLogger() {
        Logger logger = mock(LocationAwareLogger.class);
        when(logger.getName()).thenReturn("testlogger");
        InternalLogger internalLogger = Slf4JLoggerFactory.wrapLogger(logger);
        assertInstanceOf(LocationAwareSlf4JLogger.class, internalLogger);
        assertEquals("testlogger", internalLogger.name());
    }

    /**
     * Tests the formatMessage functionality of the InternalLogger wrapped by Slf4JLoggerFactory.
     * This test verifies that the correct logging messages are formatted and captured at various logging levels,
     * including DEBUG, ERROR, INFO, TRACE, and WARN.
     * <p>
     * The test initializes a mock LocationAwareLogger and configures it to return true for all log level enabled checks.
     * The logger name is set to "testlogger". It then verifies that the logger captures correctly formatted messages
     * for different log levels and different numbers of message arguments.
     * <p>
     * The captured log messages are compared with expected values ensuring correct formatting and parameter substitution.
     */
    @Test
    public void testFormatMessage() {
        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        LocationAwareLogger logger = mock(LocationAwareLogger.class);
        when(logger.isDebugEnabled()).thenReturn(true);
        when(logger.isErrorEnabled()).thenReturn(true);
        when(logger.isInfoEnabled()).thenReturn(true);
        when(logger.isTraceEnabled()).thenReturn(true);
        when(logger.isWarnEnabled()).thenReturn(true);
        when(logger.getName()).thenReturn("testlogger");

        InternalLogger internalLogger = Slf4JLoggerFactory.wrapLogger(logger);
        internalLogger.debug("{}", "debug");
        internalLogger.debug("{} {}", "debug1", "debug2");
        internalLogger.debug("{} {} {}", "debug1", "debug2", "debug3");

        internalLogger.error("{}", "error");
        internalLogger.error("{} {}", "error1", "error2");
        internalLogger.error("{} {} {}", "error1", "error2", "error3");

        internalLogger.info("{}", "info");
        internalLogger.info("{} {}", "info1", "info2");
        internalLogger.info("{} {} {}", "info1", "info2", "info3");

        internalLogger.trace("{}", "trace");
        internalLogger.trace("{} {}", "trace1", "trace2");
        internalLogger.trace("{} {} {}", "trace1", "trace2", "trace3");

        internalLogger.warn("{}", "warn");
        internalLogger.warn("{} {}", "warn1", "warn2");
        internalLogger.warn("{} {} {}", "warn1", "warn2", "warn3");

        verify(logger, times(3)).log(ArgumentMatchers.isNull(), eq(LocationAwareSlf4JLogger.FQCN),
                eq(LocationAwareLogger.DEBUG_INT), captor.capture(), any(Object[].class),
                ArgumentMatchers.isNull());
        verify(logger, times(3)).log(ArgumentMatchers.isNull(), eq(LocationAwareSlf4JLogger.FQCN),
                eq(LocationAwareLogger.ERROR_INT), captor.capture(), any(Object[].class),
                ArgumentMatchers.isNull());
        verify(logger, times(3)).log(ArgumentMatchers.isNull(), eq(LocationAwareSlf4JLogger.FQCN),
                eq(LocationAwareLogger.INFO_INT), captor.capture(), any(Object[].class),
                ArgumentMatchers.isNull());
        verify(logger, times(3)).log(ArgumentMatchers.isNull(), eq(LocationAwareSlf4JLogger.FQCN),
                eq(LocationAwareLogger.TRACE_INT), captor.capture(), any(Object[].class),
                ArgumentMatchers.isNull());
        verify(logger, times(3)).log(ArgumentMatchers.isNull(), eq(LocationAwareSlf4JLogger.FQCN),
                eq(LocationAwareLogger.WARN_INT), captor.capture(), any(Object[].class),
                ArgumentMatchers.isNull());

        Iterator<String> logMessages = captor.getAllValues().iterator();
        assertEquals("debug", logMessages.next());
        assertEquals("debug1 debug2", logMessages.next());
        assertEquals("debug1 debug2 debug3", logMessages.next());
        assertEquals("error", logMessages.next());
        assertEquals("error1 error2", logMessages.next());
        assertEquals("error1 error2 error3", logMessages.next());
        assertEquals("info", logMessages.next());
        assertEquals("info1 info2", logMessages.next());
        assertEquals("info1 info2 info3", logMessages.next());
        assertEquals("trace", logMessages.next());
        assertEquals("trace1 trace2", logMessages.next());
        assertEquals("trace1 trace2 trace3", logMessages.next());
        assertEquals("warn", logMessages.next());
        assertEquals("warn1 warn2", logMessages.next());
        assertEquals("warn1 warn2 warn3", logMessages.next());
        assertFalse(logMessages.hasNext());
    }
}
