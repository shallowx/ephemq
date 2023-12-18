package org.meteor.proxy.internal;

import org.meteor.configuration.ServerConfig;

import java.util.Properties;

public class ProxyServerConfig extends ServerConfig {
    private final ProxyConfig proxyConfiguration;
    public ProxyServerConfig(Properties properties) {
        super(properties);
        this.proxyConfiguration =new ProxyConfig(properties, this.commonConfiguration, this.zookeeperConfiguration);
    }

    public ProxyConfig getProxyConfiguration() {
        return proxyConfiguration;
    }
}
