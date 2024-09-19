package org.meteor.common.logging;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class Log4J2LoggerFactoryTest {

    /**
     * Tests the creation of a logger using the Log4J2LoggerFactory.
     * <p>
     * This test verifies that a logger instance created with the name "foo" is an instance of Log4J2Logger
     * and asserts that the logger's name matches the provided name.
     */
    @Test
    public void testCreation() {
        InternalLogger logger = Log4J2LoggerFactory.INSTANCE.newLogger("foo");
        assertInstanceOf(Log4J2Logger.class, logger);
        assertEquals("foo", logger.name());
    }
}
