package org.ephemq.config;

import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.Properties;

public class CommonConfigTest {

    @Test
    public void testCommonConfig() {
        CommonConfig commonConfig = getCommonConfig();
        Assertions.assertNotNull(commonConfig);
        Assertions.assertEquals("test-server-id", commonConfig.getServerId());
        Assertions.assertEquals("test-cluster-name", commonConfig.getClusterName());
        Assertions.assertEquals("127.0.0.1", commonConfig.getAdvertisedAddress());
        Assertions.assertEquals(10000, commonConfig.getAdvertisedPort());
        Assertions.assertEquals(10001, commonConfig.getCompatiblePort());
        Assertions.assertEquals(10000, commonConfig.getShutdownMaxWaitTimeMilliseconds());
        Assertions.assertEquals(10000, commonConfig.getAuxThreadLimit());
        Assertions.assertEquals(10000, commonConfig.getCommandHandleThreadLimit());
        Assertions.assertTrue(commonConfig.isSocketPreferEpoll());
        Assertions.assertTrue(commonConfig.isSocketPreferIoUring());
        Assertions.assertTrue(commonConfig.isSocketPreferAffinity());
        Assertions.assertTrue(commonConfig.isThreadAffinityEnabled());
        Assertions.assertTrue(commonConfig.isThreadAffinityEnabled());
    }

    private static CommonConfig getCommonConfig() {
        Properties properties = new Properties();
        properties.put("server.id", "test-server-id");
        properties.put("server.cluster.name", "test-cluster-name");
        properties.put("server.advertised.address", "127.0.0.1");
        properties.put("server.advertised.port", 10000);
        properties.put("server.compatible.port", 10001);
        properties.put("shutdown.max.wait.time.milliseconds", 10000);
        properties.put("aux.thread.limit", 10000);
        properties.put("command.handler.thread.limit", 10000);
        properties.put("socket.prefer.epoll", true);
        properties.put("socket.prefer.iouring", true);
        properties.put("socket.prefer.affinity", true);
        properties.put("thread.affinity.enabled", true);

        ServerConfig config = new ServerConfig(properties);
        return config.getCommonConfig();
    }
}
