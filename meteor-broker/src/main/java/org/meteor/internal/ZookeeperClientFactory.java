package org.meteor.internal;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.SessionConnectionStateErrorPolicy;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.config.ZookeeperConfig;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ZookeeperClientFactory {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ZookeeperClientFactory.class);
    private static final Map<String, CuratorFramework> readyClients = new ConcurrentHashMap<>();

    public static CuratorFramework getReadyClient(ZookeeperConfig config, String clusterName) {
        return readyClients.computeIfAbsent(clusterName, namespace -> {
            String url = config.getZookeeperUrl();
            if (url == null) {
                throw new IllegalStateException("Zookeeper address not found");
            }

            if (logger.isInfoEnabled()) {
                logger.info("Using url[{}] as zookeeper address", url);
            }
            CuratorFramework client = CuratorFrameworkFactory.builder()
                    .connectString(url)
                    .namespace(namespace)
                    .sessionTimeoutMs(config.getZookeeperSessionTimeoutMilliseconds())
                    .connectionTimeoutMs(config.getZookeeperConnectionTimeoutMilliseconds())
                    .connectionStateErrorPolicy(new SessionConnectionStateErrorPolicy())
                    .retryPolicy(new ExponentialBackoffRetry(config.getZookeeperConnectionRetrySleepMilliseconds(), config.getZookeeperConnectionRetries()))
                    .build();

            client.start();
            return client;
        });
    }

    public static void closeClient() {
        for (CuratorFramework client : readyClients.values()) {
            client.close();
        }
        readyClients.clear();
    }
}
