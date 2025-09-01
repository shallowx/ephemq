package org.ephemq.proxy.core;

import java.util.Properties;
import org.ephemq.config.ServerConfig;

/**
 * The ProxyServerConfig class extends the ServerConfig class to include proxy-specific configurations.
 * It initializes and provides access to proxy settings through the ProxyConfig object.
 */
public class ProxyServerConfig extends ServerConfig {
    /**
     * Holds the proxy-specific configuration settings for the server.
     * Initialized with properties from the common server configuration and Zookeeper configuration.
     */
    private final ProxyConfig proxyConfiguration;

    /**
     * Constructs a ProxyServerConfig instance and initializes various proxy-specific configuration parameters
     * along with general server configurations by utilizing the provided properties.
     *
     * @param properties The properties used to initialize proxy and server configurations.
     */
    public ProxyServerConfig(Properties properties) {
        super(properties);
        this.proxyConfiguration = new ProxyConfig(properties, this.commonConfig, this.zookeeperConfig);
    }

    /**
     * Retrieves the proxy configuration.
     *
     * @return the proxy configuration
     */
    public ProxyConfig getProxyConfiguration() {
        return proxyConfiguration;
    }
}
