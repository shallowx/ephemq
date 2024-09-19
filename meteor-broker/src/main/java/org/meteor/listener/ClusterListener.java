package org.meteor.listener;

import org.meteor.common.message.Node;

/**
 * The ClusterListener interface defines callback methods that are triggered
 * in response to various cluster-related events. Implementations of this interface
 * can be used to handle changes in the cluster state, such as nodes joining or
 * leaving the cluster, and nodes becoming either operational or non-operational.
 */
public interface ClusterListener {
    /**
     * Callback method that is triggered when a node gains the controller role within the cluster.
     *
     * @param node the node that has obtained the controller role.
     */
    void onGetControlRole(Node node);

    /**
     * This method is called when the current node loses its controller role within the cluster.
     *
     * @param node The node that lost the controller role.
     */
    void onLostControlRole(Node node);

    /**
     * Callback method invoked when a node joins the cluster. This method is used to handle
     * any necessary operations or updates required when a new node becomes part of the cluster.
     *
     * @param node the instance of the Node that has joined the cluster
     */
    void onNodeJoin(Node node);

    /**
     * Callback method that is triggered when a node is detected to be down or non-operational.
     *
     * @param node the node that is down or non-operational
     */
    void onNodeDown(Node node);

    /**
     * Called when a node leaves the cluster.
     *
     * @param node the node that is leaving the cluster
     */
    void onNodeLeave(Node node);
}
