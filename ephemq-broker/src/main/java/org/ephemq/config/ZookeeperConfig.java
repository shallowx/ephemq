package org.ephemq.config;

import java.util.Properties;

import static org.ephemq.common.util.ObjectLiteralsTransformUtil.object2Int;
import static org.ephemq.common.util.ObjectLiteralsTransformUtil.object2String;

/**
 * A configuration class for managing Zookeeper-related settings.
 * <p>
 * This class provides methods to retrieve various Zookeeper configuration properties
 * from a specified {@link Properties} object.
 */
public class ZookeeperConfig {
    /**
     * Configuration key for Zookeeper URL.
     * Represents the address of the Zookeeper server(s), typically used to specify
     * the hostname(s) and port(s) for connecting to Zookeeper. Defaults to "localhost:2181"
     * if not specified in the configuration properties.
     */
    private static final String ZOOKEEPER_URL = "zookeeper.url";
    /**
     * Configuration key for specifying the sleep duration in milliseconds
     * between retries when attempting to connect to a ZooKeeper server.
     */
    private static final String ZOOKEEPER_CONNECTION_RETRY_SLEEP_MILLISECONDS = "zookeeper.connection.retry.sleep.milliseconds";
    /**
     * Defines the property key for the maximum number of retry attempts to establish a
     * connection to the Zookeeper service.
     */
    private static final String ZOOKEEPER_CONNECTION_RETRIES = "zookeeper.connection.retries";
    /**
     * Configuration property key for specifying the timeout duration in milliseconds
     * for establishing a connection to the Zookeeper ensemble.
     */
    private static final String ZOOKEEPER_CONNECTION_TIMEOUT_MILLISECONDS = "zookeeper.connection.timeout.milliseconds";
    /**
     * The property key for the Zookeeper session timeout in milliseconds.
     * This configuration parameter defines the maximum time that Zookeeper will wait before timing out an inactive session.
     */
    private static final String ZOOKEEPER_SESSION_TIMEOUT_MILLISECONDS = "zookeeper.session.timeout.milliseconds";
    /**
     * A Properties object that holds configuration settings for Zookeeper.
     */
    private final Properties prop;

    /**
     * Constructs a ZookeeperConfig instance using the provided properties.
     *
     * @param prop The properties used to initialize Zookeeper configuration settings.
     */
    public ZookeeperConfig(Properties prop) {
        this.prop = prop;
    }

    /**
     * Retrieves the Zookeeper URL from the configuration properties.
     * If the Zookeeper URL is not set in the properties, it defaults to "localhost:2181".
     *
     * @return the configured Zookeeper URL or the default value if not set.
     */
    public String getZookeeperUrl() {
        return object2String(prop.getOrDefault(ZOOKEEPER_URL, "localhost:2181"));
    }

    /**
     * Retrieves the sleep time in milliseconds between retry attempts for connecting to Zookeeper.
     *
     * @return the sleep time in milliseconds for Zookeeper connection retry attempts.
     */
    public int getZookeeperConnectionRetrySleepMilliseconds() {
        return object2Int(prop.getOrDefault(ZOOKEEPER_CONNECTION_RETRY_SLEEP_MILLISECONDS, 30000));
    }

    /**
     * Retrieves the number of retries for the Zookeeper connection.
     *
     * @return the number of connection retries, defaulting to 3 if not specified.
     */
    public int getZookeeperConnectionRetries() {
        return object2Int(prop.getOrDefault(ZOOKEEPER_CONNECTION_RETRIES, 3));
    }

    /**
     * Retrieves the ZooKeeper connection timeout in milliseconds.
     * If the property is not set, a default value of 5000 milliseconds is used.
     *
     * @return the ZooKeeper connection timeout in milliseconds
     */
    public int getZookeeperConnectionTimeoutMilliseconds() {
        return object2Int(prop.getOrDefault(ZOOKEEPER_CONNECTION_TIMEOUT_MILLISECONDS, 5000));
    }

    /**
     * Returns the Zookeeper session timeout in milliseconds. The value is taken from the
     * properties, and if not specified, a default value of 30000 milliseconds is used.
     *
     * @return the session timeout in milliseconds
     */
    public int getZookeeperSessionTimeoutMilliseconds() {
        return object2Int(prop.getOrDefault(ZOOKEEPER_SESSION_TIMEOUT_MILLISECONDS, 30000));
    }
}
