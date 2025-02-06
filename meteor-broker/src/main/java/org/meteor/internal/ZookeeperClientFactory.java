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

/**
 * Factory class for managing and providing CuratorFramework clients for Zookeeper clusters.
 */
public class ZookeeperClientFactory {
    /**
     * Logger instance for logging messages in the ZookeeperClientFactory class.
     * <p>
     * This logger is created using the InternalLoggerFactory and is intended
     * to facilitate logging within the context of the ZookeeperClientFactory.
     */
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ZookeeperClientFactory.class);
    /**
     * A map that holds instances of {@link CuratorFramework} clients keyed by cluster names.
     * <p>
     * This map ensures that there is at most one CuratorFramework client instance per cluster name,
     * and is used to manage ready-to-use Zookeeper clients.
     */
    private static final Map<String, CuratorFramework> readyClients = new ConcurrentHashMap<>();

    /**
     * Retrieves a ready-to-use CuratorFramework client for a specified Zookeeper cluster.
     * If a client for the specified cluster name does not already exist, this method creates and starts a new one.
     *
     * @param config Configuration object containing Zookeeper settings.
     * @param clusterName The name of the Zookeeper cluster for which the client is needed.
     * @return A ready-to-use {@link CuratorFramework} client.
     */
    public static CuratorFramework getReadyClient(ZookeeperConfig config, String clusterName) {
        return readyClients.computeIfAbsent(clusterName, namespace -> {
            String url = config.getZookeeperUrl();
            if (url == null) {
                throw new IllegalStateException("Zookeeper cluster address not found");
            }

            if (logger.isInfoEnabled()) {
                logger.info(STR."Using url[\{url}] as zookeeper address");
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

    /**
     * Closes all active instances of {@link CuratorFramework} clients managed by this factory.
     * <p>
     * This method iterates through all entries in the {@code readyClients} map,
     * closes each {@link CuratorFramework} instance, and then clears the map.
     */
    public static void closeClient() {
        for (CuratorFramework client : readyClients.values()) {
            client.close();
        }
        readyClients.clear();
    }
}
