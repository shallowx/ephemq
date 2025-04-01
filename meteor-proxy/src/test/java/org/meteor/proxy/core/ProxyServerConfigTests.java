package org.meteor.proxy.core;

import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.Properties;

public class ProxyServerConfigTests {

    @Test
    public void testProxyServerConfig() {
        Properties properties = new Properties();
        properties.put("proxy.upstream.servers", "127.0.0.1:10000");
        ProxyServerConfig proxyServerConfig = new ProxyServerConfig(properties);
        Assertions.assertNotNull(proxyServerConfig);
        Assertions.assertNotNull(proxyServerConfig.getProxyConfiguration());
        Assertions.assertEquals("127.0.0.1:10000", proxyServerConfig.getProxyConfiguration().getProxyUpstreamServers());
        Assertions.assertEquals(200000, proxyServerConfig.getProxyConfiguration().getProxyHeavyLoadSubscriberThreshold());
    }
}
