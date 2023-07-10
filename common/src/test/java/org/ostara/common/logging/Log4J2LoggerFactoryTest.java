package org.ostara.common.logging;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class Log4J2LoggerFactoryTest {

    @Test
    public void testCreation() {
        InternalLogger logger = Log4J2LoggerFactory.INSTANCE.newLogger("foo");
        assertTrue(logger instanceof Log4J2Logger);
        assertEquals("foo", logger.name());
    }
}
