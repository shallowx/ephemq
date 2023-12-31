package org.meteor.config;

import java.util.Properties;
import static org.meteor.common.util.ObjectLiteralsTransformUtil.object2Int;
import static org.meteor.common.util.ObjectLiteralsTransformUtil.object2String;

public class ZookeeperConfig {
    private static final String ZOOKEEPER_URL = "zookeeper.url";
    private static final String ZOOKEEPER_CONNECTION_RETRY_SLEEP_MILLISECONDS = "zookeeper.connection.retry.sleep.milliseconds";
    private static final String ZOOKEEPER_CONNECTION_RETRIES = "zookeeper.connection.retries";
    private static final String ZOOKEEPER_CONNECTION_TIMEOUT_MILLISECONDS = "zookeeper.connection.timeout.milliseconds";
    private static final String ZOOKEEPER_SESSION_TIMEOUT_MILLISECONDS = "zookeeper.session.timeout.milliseconds";
    private final Properties prop;

    public ZookeeperConfig(Properties prop) {
        this.prop = prop;
    }

    public String getZookeeperUrl() {
        return object2String(prop.getOrDefault(ZOOKEEPER_URL, "localhost:2181"));
    }

    public int getZookeeperConnectionRetrySleepMilliseconds() {
        return object2Int(prop.getOrDefault(ZOOKEEPER_CONNECTION_RETRY_SLEEP_MILLISECONDS, 30000));
    }

    public int getZookeeperConnectionRetries() {
        return object2Int(prop.getOrDefault(ZOOKEEPER_CONNECTION_RETRIES, 3));
    }

    public int getZookeeperConnectionTimeoutMilliseconds() {
        return object2Int(prop.getOrDefault(ZOOKEEPER_CONNECTION_TIMEOUT_MILLISECONDS, 5000));
    }

    public int getZookeeperSessionTimeoutMilliseconds() {
        return object2Int(prop.getOrDefault(ZOOKEEPER_SESSION_TIMEOUT_MILLISECONDS, 30000));
    }
}
