package org.meteor.config;

import org.meteor.common.util.TypeTransformUtils;

import java.util.Properties;

public class ZookeeperConfig {
    private static final String ZOOKEEPER_URL = "zookeeper.url";
    private static final String ZOOKEEPER_CONNECTION_RETRY_SLEEP_MS = "zookeeper.connection.retry.sleep.ms";
    private static final String ZOOKEEPER_CONNECTION_RETRIES = "zookeeper.connection.retries";
    private static final String ZOOKEEPER_CONNECTION_TIMEOUT_MS = "zookeeper.connection.timeout.ms";
    private static final String ZOOKEEPER_SESSION_TIMEOUT_MS = "zookeeper.session.timeout.ms";
    private final Properties prop;

    public ZookeeperConfig(Properties prop) {
        this.prop = prop;
    }

    public String getZookeeperUrl() {
        return TypeTransformUtils.object2String(prop.getOrDefault(ZOOKEEPER_URL, "localhost:2181"));
    }

    public int getZookeeperConnectionRetrySleepMs() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(ZOOKEEPER_CONNECTION_RETRY_SLEEP_MS, 30000));
    }

    public int getZookeeperConnectionRetries() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(ZOOKEEPER_CONNECTION_RETRIES, 3));
    }

    public int getZookeeperConnectionTimeoutMs() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(ZOOKEEPER_CONNECTION_TIMEOUT_MS, 5000));
    }

    public int getZookeeperSessionTimeoutMs() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(ZOOKEEPER_SESSION_TIMEOUT_MS, 30000));
    }
}
