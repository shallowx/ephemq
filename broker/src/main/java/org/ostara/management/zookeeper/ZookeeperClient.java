package org.ostara.management.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.SessionConnectionStateErrorPolicy;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.core.Config;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ZookeeperClient {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ZookeeperClient.class);

    private static final Map<String, CuratorFramework> clients = new ConcurrentHashMap<>();

    public static CuratorFramework getClient(Config config, String clusterName) {
        return clients.computeIfAbsent(clusterName, namespace -> {
            String url = config.getZookeeperUrl();
            if (url == null) {
                throw new IllegalStateException("Zookeeper address not found");
            }
            logger.info("Using {} as zookeeper address", url);

            CuratorFramework client = CuratorFrameworkFactory.builder()
                    .connectString(url)
                    .namespace(namespace)
                    .sessionTimeoutMs(config.getZookeeperSessionTimeoutMs())
                    .connectionTimeoutMs(config.getZookeeperConnectionTimeoutMs())
                    .connectionStateErrorPolicy(new SessionConnectionStateErrorPolicy())
                    .retryPolicy(new ExponentialBackoffRetry(config.getZookeeperConnectionRetrySleepMs(), config.getZookeeperConnectionRetries()))
                    .build();

            client.start();
            return client;
        });
    }

    public static void closeClient() {
        for (CuratorFramework client : clients.values()) {
            client.close();
        }
        clients.clear();;
    }
}
