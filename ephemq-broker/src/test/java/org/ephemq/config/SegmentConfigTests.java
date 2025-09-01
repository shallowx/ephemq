package org.ephemq.config;

import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.Properties;

public class SegmentConfigTests {

    @Test
    public void testSegmentConfig() {
        Properties properties = new Properties();
        properties.setProperty("segment.rolling.size", "1");
        properties.setProperty("segment.retain.limit", "1");
        properties.setProperty("segment.retain.time.milliseconds", "1");

        ServerConfig serverConfig = new ServerConfig(properties);
        SegmentConfig segmentConfig = serverConfig.getSegmentConfig();

        Assertions.assertNotNull(segmentConfig);
        Assertions.assertEquals(1, segmentConfig.getSegmentRollingSize());
        Assertions.assertEquals(1, segmentConfig.getSegmentRetainLimit());
        Assertions.assertEquals(1, segmentConfig.getSegmentRetainTimeMilliseconds());
    }
}
