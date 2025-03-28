package org.meteor.config;

import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.Properties;

public class DefaultDispatchConfigTests {

    @Test
    public void testDispatchConfig() {
        Properties properties = new Properties();
        properties.put("dispatch.entry.load.limit", 10000);
        properties.put("dispatch.entry.follow.limit", 10000);
        properties.put("dispatch.entry.pursue.limit", 10000);
        properties.put("dispatch.entry.align.limit", 10000);
        properties.put("dispatch.entry.pursue.timeout.milliseconds", 10000);

        ServerConfig config = new ServerConfig(properties);
        DefaultDispatchConfig defaultDispatchConfig = config.getRecordDispatchConfig();
        Assertions.assertNotNull(defaultDispatchConfig);
        Assertions.assertEquals(10000, defaultDispatchConfig.getDispatchEntryLoadLimit());
        Assertions.assertEquals(10000, defaultDispatchConfig.getDispatchEntryFollowLimit());
        Assertions.assertEquals(10000, defaultDispatchConfig.getDispatchEntryPursueLimit());
        Assertions.assertEquals(10000, defaultDispatchConfig.getDispatchEntryAlignLimit());
        Assertions.assertEquals(10000, defaultDispatchConfig.getDispatchEntryPursueTimeoutMilliseconds());
    }
}
