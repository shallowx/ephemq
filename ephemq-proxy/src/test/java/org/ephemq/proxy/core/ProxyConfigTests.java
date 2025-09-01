package org.ephemq.proxy.core;

import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.ephemq.config.CommonConfig;
import org.ephemq.config.ZookeeperConfig;

import java.util.Properties;

public class ProxyConfigTests {

    @Test
    public void testProxyConfig() {
        Properties prop = new Properties();
        prop.put("proxy.upstream.servers", "127.0.0.1:10000");
        CommonConfig commonConfiguration = new CommonConfig(prop);
        ZookeeperConfig zookeeperConfiguration = new ZookeeperConfig(prop);
        ProxyConfig proxyConfig = new ProxyConfig(prop, commonConfiguration, zookeeperConfiguration);
        Assert.assertEquals(commonConfiguration, proxyConfig.getCommonConfiguration());
        Assertions.assertEquals("127.0.0.1:10000", proxyConfig.getProxyUpstreamServers());
        Assertions.assertEquals(200000, proxyConfig.getProxyHeavyLoadSubscriberThreshold());
    }
}
