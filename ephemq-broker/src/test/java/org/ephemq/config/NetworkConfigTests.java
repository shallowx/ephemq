package org.ephemq.config;

import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.Properties;

public class NetworkConfigTests {

    @Test
    public void testNetworkConfig() {
        Properties prop = new Properties();
        prop.put("connection.timeout.milliseconds", 10000);
        prop.put("network.log.debug.enabled", "true");
        prop.put("socket.write.buffer.high.watermark", 10000);

        ServerConfig serverConfig = new ServerConfig(prop);
        NetworkConfig networkConfig = serverConfig.getNetworkConfig();
        Assertions.assertNotNull(networkConfig);

        Assertions.assertEquals(10000, networkConfig.getConnectionTimeoutMilliseconds());
        Assertions.assertTrue(networkConfig.isNetworkLogDebugEnabled());
        Assertions.assertEquals(10000, networkConfig.getWriteBufferWaterMarker());
        Assertions.assertEquals(1, networkConfig.getIoThreadLimit());
        Assertions.assertEquals(100, networkConfig.getNotifyClientTimeoutMilliseconds());
    }
}
