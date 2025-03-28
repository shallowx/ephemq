package org.meteor.config;


import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.Properties;

public class MessageConfigTests {

    @Test
    public void testMessageConfig() {
        Properties properties = new Properties();
        properties.put("message.sync.thread.limit", 10000);
        properties.put("message.storage.thread.limit", 10000);
        properties.put("message.dispatch.thread.limit", 10000);

        ServerConfig serverConfig = new ServerConfig(properties);
        MessageConfig messageConfig = serverConfig.getMessageConfig();

        Assertions.assertNotNull(messageConfig);
        Assertions.assertEquals(10000, messageConfig.getMessageSyncThreadLimit());
        Assertions.assertEquals(10000, messageConfig.getMessageStorageThreadLimit());
        Assertions.assertEquals(10000, messageConfig.getMessageDispatchThreadLimit());
    }
}
