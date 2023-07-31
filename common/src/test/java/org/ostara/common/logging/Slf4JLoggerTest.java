package org.ostara.common.logging;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

public class Slf4JLoggerTest {
    private static final Exception e = new Exception();

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

    @Test
    public void testTrace() {
        Logger mockLogger = mock(Logger.class);

        when(mockLogger.getName()).thenReturn("foo");

        InternalLogger logger = new Slf4JLogger(mockLogger);
        logger.trace("a");

        verify(mockLogger).getName();
        verify(mockLogger).trace("a");
    }

    @Test
    public void testTraceWithException() {
        Logger mockLogger = mock(Logger.class);

        when(mockLogger.getName()).thenReturn("foo");

        InternalLogger logger = new Slf4JLogger(mockLogger);
        logger.trace("a", e);

        verify(mockLogger).getName();
        verify(mockLogger).trace("a", e);
    }

    @Test
    public void testDebug() {
        Logger mockLogger = mock(Logger.class);

        when(mockLogger.getName()).thenReturn("foo");

        InternalLogger logger = new Slf4JLogger(mockLogger);
        logger.debug("a");

        verify(mockLogger).getName();
        verify(mockLogger).debug("a");
    }

    @Test
    public void testDebugWithException() {
        Logger mockLogger = mock(Logger.class);

        when(mockLogger.getName()).thenReturn("foo");

        InternalLogger logger = new Slf4JLogger(mockLogger);
        logger.debug("a", e);

        verify(mockLogger).getName();
        verify(mockLogger).debug("a", e);
    }

    @Test
    public void testInfo() {
        Logger mockLogger = mock(Logger.class);

        when(mockLogger.getName()).thenReturn("foo");

        InternalLogger logger = new Slf4JLogger(mockLogger);
        logger.info("a");

        verify(mockLogger).getName();
        verify(mockLogger).info("a");
    }

    @Test
    public void testInfoWithException() {
        Logger mockLogger = mock(Logger.class);

        when(mockLogger.getName()).thenReturn("foo");

        InternalLogger logger = new Slf4JLogger(mockLogger);
        logger.info("a", e);

        verify(mockLogger).getName();
        verify(mockLogger).info("a", e);
    }

    @Test
    public void testWarn() {
        Logger mockLogger = mock(Logger.class);

        when(mockLogger.getName()).thenReturn("foo");

        InternalLogger logger = new Slf4JLogger(mockLogger);
        logger.warn("a");

        verify(mockLogger).getName();
        verify(mockLogger).warn("a");
    }

    @Test
    public void testWarnWithException() {
        Logger mockLogger = mock(Logger.class);

        when(mockLogger.getName()).thenReturn("foo");

        InternalLogger logger = new Slf4JLogger(mockLogger);
        logger.warn("a", e);

        verify(mockLogger).getName();
        verify(mockLogger).warn("a", e);
    }

    @Test
    public void testError() {
        Logger mockLogger = mock(Logger.class);

        when(mockLogger.getName()).thenReturn("foo");

        InternalLogger logger = new Slf4JLogger(mockLogger);
        logger.error("a");

        verify(mockLogger).getName();
        verify(mockLogger).error("a");
    }

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
