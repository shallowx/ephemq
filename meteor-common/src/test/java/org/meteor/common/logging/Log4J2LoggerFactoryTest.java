package org.meteor.common.logging;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class Log4J2LoggerFactoryTest {

    @Test
    public void testCreation() {
        InternalLogger logger = Log4J2LoggerFactory.INSTANCE.newLogger("foo");
        assertInstanceOf(Log4J2Logger.class, logger);
        assertEquals("foo", logger.name());
    }
}
