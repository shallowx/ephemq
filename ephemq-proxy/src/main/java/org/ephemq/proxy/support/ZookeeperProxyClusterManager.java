package org.ephemq.proxy.support;

import java.util.Set;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.zookeeper.data.Stat;
import org.ephemq.common.logging.InternalLogger;
import org.ephemq.common.logging.InternalLoggerFactory;
import org.ephemq.common.message.Node;
import org.ephemq.zookeeper.ZookeeperClientFactory;
import org.ephemq.listener.ClusterListener;
import org.ephemq.proxy.core.ProxyConfig;
import org.ephemq.proxy.core.ProxyServerConfig;
import org.ephemq.zookeeper.ZookeeperClusterManager;

/**
 * ZookeeperProxyClusterManager is a specialized cluster manager that manages
 * a cluster of proxy nodes using Zookeeper. It extends the ZookeeperClusterManager
 * and implements ClusterListener and ProxyClusterManager.
 * <p>
 * This class handles node registration, connection state management, consistent
 * hashing for routing, and provides hooks for node join, leave, and down events.
 */
class ZookeeperProxyClusterManager extends ZookeeperClusterManager implements ClusterListener, ProxyClusterManager {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ZookeeperProxyClusterManager.class);
    /**
     * Represents a consistent hashing ring that facilitates the distribution of keys across multiple nodes.
     * It uses virtual nodes to ensure a more balanced distribution and aims to minimize key reassignment
     * when nodes are added or removed. This implementation leverages a navigable map and synchronized
     * read/write operations to provide thread-safe access to the ring structure.
     */
    private final ConsistentHashingRing hashingRing;
    /**
     * Configuration settings for the proxy server.
     */
    private final ProxyConfig proxyConfiguration;

    /**
     * Initializes a ZookeeperProxyClusterManager instance with the specified configuration.
     *
     * @param configuration The configuration settings for the proxy server, including the Zookeeper configuration settings.
     */
    public ZookeeperProxyClusterManager(ProxyServerConfig configuration) {
        super(configuration, null);
        this.proxyConfiguration = configuration.getProxyConfiguration();
        this.client = ZookeeperClientFactory.getReadyClient(proxyConfiguration.getZookeeperConfiguration(), proxyConfiguration.getCommonConfiguration().getClusterName());
        this.hashingRing = new ConsistentHashingRing();
        this.listeners.add(this);
    }

    /**
     * Starts the Zookeeper proxy cluster manager by initializing the connection state listener,
     * starting the brokers listener, and registering the node in the Zookeeper cluster.
     *
     * @throws Exception if there is an error during the startup process.
     */
    @Override
    public void start() throws Exception {
        connectionStateListener = (client, newState) -> {
            if (newState == ConnectionState.RECONNECTED) {
                try {
                    Stat stat = client.checkExists().forPath(ZookeeperProxyPathConstants.PROXIES_ID.formatted(proxyConfiguration.getCommonConfiguration().getServerId()));
                    if (stat != null) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Zookeeper proxy cluster manager has been started.");
                        }
                        return;
                    }
                    registerNode(ZookeeperProxyPathConstants.PROXIES_ID);
                } catch (Exception e) {
                    if (logger.isErrorEnabled()) {
                        logger.error(e.getMessage(), e);
                    }
                }
            }
        };
        this.client.getConnectionStateListenable().addListener(connectionStateListener);
        startBrokersListener(ZookeeperProxyPathConstants.PROXIES_IDS);
        registerNode(ZookeeperProxyPathConstants.PROXIES_ID);
    }

    /**
     * Routes the given key to a set of nodes in the consistent hashing ring.
     *
     * @param key the key to be routed
     * @param size the number of nodes to be returned
     * @return a set of nodes that the key is routed to; an empty set if size is less than 1 or no nodes exist
     */
    @Override
    public Set<String> route2Nodes(String key, int size) {
        return hashingRing.route2Nodes(key, size);
    }

    /**
     * Determines if the current node is acting as the controller within the cluster.
     *
     * @return true if the current node is the controller, false otherwise.
     */
    @Override
    public boolean isController() {
        throw new UnsupportedOperationException();
    }

    /**
     * Handles the event when a node gains control role within the cluster.
     *
     * @param node The {@link Node} instance that has been assigned the control role.
     */
    @Override
    public void onGetControlRole(Node node) {
    }

    /**
     * This method is called when the node loses its control role within the cluster.
     *
     * @param node the node that has lost its control role
     */
    @Override
    public void onLostControlRole(Node node) {
    }

    /**
     * This method is called when a node joins the cluster. It inserts the node's ID
     * into the hashing ring for consistent hashing.
     *
     * @param node The node that has joined the cluster. If the node is not null, its ID
     *             will be inserted into the hashing ring for consistent hashing.
     */
    @Override
    public void onNodeJoin(Node node) {
        if (node != null) {
            hashingRing.insertNode(node.getId());
        }
    }

    /**
     * Handles the event when a node in the cluster goes down.
     * If the provided node is not null, this method removes the node from
     * the hashing ring using its unique identifier.
     *
     * @param node The Node instance that has gone down. If null, no action is taken.
     */
    @Override
    public void onNodeDown(Node node) {
        if (node != null) {
            hashingRing.deleteNode(node.getId());
        }
    }

    /**
     * Called when a node leaves the cluster. This method handles the removal of a node from the hashing ring.
     *
     * @param node the node that is leaving the cluster
     */
    @Override
    public void onNodeLeave(Node node) {
        if (node != null) {
            hashingRing.deleteNode(node.getId());
        }
    }

    /**
     * Shuts down the ZookeeperProxyClusterManager instance by unregistering the proxy node,
     * closing the cache, and removing the connection state listener.
     *
     * @throws Exception if an error occurs during any of the shutdown processes.
     */
    @Override
    public void shutdown() throws Exception {
        unregistered(ZookeeperProxyPathConstants.PROXIES_ID);
        if (cache != null) {
            cache.close();
        }
        this.client.getConnectionStateListenable().removeListener(connectionStateListener);
    }
}
