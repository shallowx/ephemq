package org.ephemq.zookeeper;

import java.util.List;
import org.ephemq.common.message.Node;
import org.ephemq.listener.ClusterListener;

/**
 * ClusterManager defines a contract for managing a cluster of nodes.
 * Implementations of this interface handle the lifecycle, state, and
 * interactions among nodes in a cluster.
 */
public interface ClusterManager {
    /**
     * Initiates the start-up process for the cluster manager. This method is
     * responsible for bootstrapping the configuration, establishing necessary
     * connections, and preparing the cluster for operations. It may involve
     * initializing client configurations, starting internal clients, and
     * invoking the start method on dependent components.
     *
     * @throws Exception if any error occurs during the start-up process.
     */
    void start() throws Exception;

    /**
     * Retrieves a list of all nodes currently in the cluster.
     *
     * @return a list of Node objects representing all nodes in the cluster
     */
    List<Node> getClusterNodes();

    /**
     * Retrieves a list of nodes that are ready and available in the cluster.
     *
     * @return a list of nodes that have a state indicating they are ready for use in the cluster.
     */
    List<Node> getClusterReadyNodes();

    /**
     * Retrieves a node in the cluster that is in a ready state based on the given node identifier.
     *
     * @param id the identifier of the node to be retrieved
     * @return the node that matches the provided identifier and is in a ready state, or null if no such node is found
     */
    Node getClusterReadyNode(String id);

    /**
     * Retrieves the current node within the cluster managed by this instance.
     *
     * @return the current Node instance representing this node in the cluster.
     */
    Node getThisNode();

    /**
     * Shuts down the cluster manager and any associated resources, ensuring that all nodes
     * in the cluster are properly terminated and any necessary cleanup is performed.
     *
     * @throws Exception if an error occurs during the shutdown process.
     */
    void shutdown() throws Exception;

    /**
     * Retrieves the identifier of the current controller node in the cluster.
     *
     * @return A string representing the identifier of the controller node.
     * @throws Exception If an error occurs while trying to retrieve the controller.
     */
    String getController() throws Exception;

    /**
     * Checks if the current node has the controller role in the cluster.
     *
     * @return true if the current node is the controller; false otherwise.
     */
    boolean isController();

    /**
     * Adds a ClusterListener to the ClusterManager. This listener will be
     * notified of various cluster-related events such as nodes joining or
     * leaving the cluster, and nodes changing their operational state.
     *
     * @param listener The ClusterListener instance that will handle the
     *                 cluster-related events.
     */
    void addClusterListener(ClusterListener listener);

    /**
     * Retrieves the name of the cluster.
     *
     * @return the name of the cluster
     */
    String getClusterName();
}
