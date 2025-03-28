package org.meteor.config;

import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.Properties;

public class ZookeeperConfigTests {

    @Test
    public void testZookeeperConfig() {
        Properties prop = new Properties();
        prop.put("zookeeper.url", "localhost:2181");
        prop.put("zookeeper.connection.retry.sleep.milliseconds", 1000);
        prop.put("zookeeper.connection.retries", 1);
        prop.put("zookeeper.connection.timeout.milliseconds", 1000);

        ServerConfig serverConfig = new ServerConfig(prop);
        ZookeeperConfig zookeeperConfig = serverConfig.getZookeeperConfig();

        Assert.assertNotNull(zookeeperConfig);
        Assertions.assertEquals("localhost:2181", zookeeperConfig.getZookeeperUrl());
        Assertions.assertEquals(1000, zookeeperConfig.getZookeeperConnectionRetrySleepMilliseconds());
        Assertions.assertEquals(1, zookeeperConfig.getZookeeperConnectionRetries());
        Assertions.assertEquals(1000, zookeeperConfig.getZookeeperConnectionTimeoutMilliseconds());
    }
}
