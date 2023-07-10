package org.ostara.common.logging;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class InternalLoggerFactoryTest {
    private static final Exception e = new Exception();
    private InternalLoggerFactory oldLoggerFactory;
    private InternalLogger mockLogger;

    @BeforeEach
    public void init() {
        oldLoggerFactory = InternalLoggerFactory.getDefaultFactory();

        final InternalLoggerFactory mockFactory = mock(InternalLoggerFactory.class);
        mockLogger = mock(InternalLogger.class);
        when(mockFactory.newLogger("mock")).thenReturn(mockLogger);
        InternalLoggerFactory.setDefaultFactory(mockFactory);
    }

    @AfterEach
    public void destroy() {
        reset(mockLogger);
        InternalLoggerFactory.setDefaultFactory(oldLoggerFactory);
    }

    @Test
    public void shouldNotAllowNullDefaultFactory() {
        assertThrows(NullPointerException.class, new Executable() {
            @Override
            public void execute() {
                InternalLoggerFactory.setDefaultFactory(null);
            }
        });
    }

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

    @Test
    public void testIsTraceEnabled() {
        when(mockLogger.isTraceEnabled()).thenReturn(true);

        InternalLogger logger = InternalLoggerFactory.getLogger("mock");
        assertTrue(logger.isTraceEnabled());
        verify(mockLogger).isTraceEnabled();
    }

    @Test
    public void testIsDebugEnabled() {
        when(mockLogger.isDebugEnabled()).thenReturn(true);

        InternalLogger logger = InternalLoggerFactory.getLogger("mock");
        assertTrue(logger.isDebugEnabled());
        verify(mockLogger).isDebugEnabled();
    }

    @Test
    public void testIsInfoEnabled() {
        when(mockLogger.isInfoEnabled()).thenReturn(true);

        InternalLogger logger = InternalLoggerFactory.getLogger("mock");
        assertTrue(logger.isInfoEnabled());
        verify(mockLogger).isInfoEnabled();
    }

    @Test
    public void testIsWarnEnabled() {
        when(mockLogger.isWarnEnabled()).thenReturn(true);

        InternalLogger logger = InternalLoggerFactory.getLogger("mock");
        assertTrue(logger.isWarnEnabled());
        verify(mockLogger).isWarnEnabled();
    }

    @Test
    public void testIsErrorEnabled() {
        when(mockLogger.isErrorEnabled()).thenReturn(true);

        InternalLogger logger = InternalLoggerFactory.getLogger("mock");
        assertTrue(logger.isErrorEnabled());
        verify(mockLogger).isErrorEnabled();
    }

    @Test
    public void testTrace() {
        final InternalLogger logger = InternalLoggerFactory.getLogger("mock");
        logger.trace("a");
        verify(mockLogger).trace("a");
    }

    @Test
    public void testTraceWithException() {
        final InternalLogger logger = InternalLoggerFactory.getLogger("mock");
        logger.trace("a", e);
        verify(mockLogger).trace("a", e);
    }

    @Test
    public void testDebug() {
        final InternalLogger logger = InternalLoggerFactory.getLogger("mock");
        logger.debug("a");
        verify(mockLogger).debug("a");
    }

    @Test
    public void testDebugWithException() {
        final InternalLogger logger = InternalLoggerFactory.getLogger("mock");
        logger.debug("a", e);
        verify(mockLogger).debug("a", e);
    }

    @Test
    public void testInfo() {
        final InternalLogger logger = InternalLoggerFactory.getLogger("mock");
        logger.info("a");
        verify(mockLogger).info("a");
    }

    @Test
    public void testInfoWithException() {
        final InternalLogger logger = InternalLoggerFactory.getLogger("mock");
        logger.info("a", e);
        verify(mockLogger).info("a", e);
    }

    @Test
    public void testWarn() {
        final InternalLogger logger = InternalLoggerFactory.getLogger("mock");
        logger.warn("a");
        verify(mockLogger).warn("a");
    }

    @Test
    public void testWarnWithException() {
        final InternalLogger logger = InternalLoggerFactory.getLogger("mock");
        logger.warn("a", e);
        verify(mockLogger).warn("a", e);
    }

    @Test
    public void testError() {
        final InternalLogger logger = InternalLoggerFactory.getLogger("mock");
        logger.error("a");
        verify(mockLogger).error("a");
    }

    @Test
    public void testErrorWithException() {
        final InternalLogger logger = InternalLoggerFactory.getLogger("mock");
        logger.error("a", e);
        verify(mockLogger).error("a", e);
    }
}
