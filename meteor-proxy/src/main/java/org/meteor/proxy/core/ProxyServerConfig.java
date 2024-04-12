package org.meteor.proxy.core;

import java.util.Properties;
import org.meteor.config.ServerConfig;

public class ProxyServerConfig extends ServerConfig {
    private final ProxyConfig proxyConfiguration;

    public ProxyServerConfig(Properties properties) {
        super(properties);
        this.proxyConfiguration = new ProxyConfig(properties, this.commonConfig, this.zookeeperConfig);
    }

    public ProxyConfig getProxyConfiguration() {
        return proxyConfiguration;
    }
}
