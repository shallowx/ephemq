package org.ephemq.proxy.support;

import java.util.Set;
import org.ephemq.zookeeper.ClusterManager;

/**
 * ProxyClusterManager extends the ClusterManager interface and provides additional
 * functionality for routing keys to a set of nodes.
 * <p>
 * Implementations of this interface will manage the distribution of keys across
 * nodes in a cluster, ensuring optimal load balancing and fault tolerance.
 */
public interface ProxyClusterManager extends ClusterManager {
    /**
     * Routes the given key to a specified number of nodes in the cluster.
     *
     * @param key The key that needs to be routed.
     * @param size The number of nodes to which the key will be routed.
     * @return A set of node identifiers to which the key is routed.
     */
    Set<String> route2Nodes(String key, int size);
}
