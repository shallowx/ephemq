package org.meteor.proxy.internal;

import org.meteor.configuration.ServerConfiguration;

import java.util.Properties;

public class ProxyServerConfiguration extends ServerConfiguration {
    private final ProxyConfiguration proxyConfiguration;
    public ProxyServerConfiguration(Properties properties) {
        super(properties);
        this.proxyConfiguration =new ProxyConfiguration(properties, this.commonConfiguration, this.zookeeperConfiguration);
    }

    public ProxyConfiguration getProxyConfiguration() {
        return proxyConfiguration;
    }
}
