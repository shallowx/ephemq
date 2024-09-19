/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.meteor.common.logging;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class Log4JLoggerFactoryTest {

    /**
     * Tests the creation of a logger instance using the Log4JLoggerFactory.
     * This test verifies that the created logger is an instance of Log4JLogger
     * and that its name is set correctly to "foo".
     */
    @Test
    public void testCreation() {
        InternalLogger logger = Log4JLoggerFactory.INSTANCE.newLogger("foo");
        assertInstanceOf(Log4JLogger.class, logger);
        assertEquals("foo", logger.name());
    }
}
