package org.meteor.coordinatio;

import org.meteor.listener.ClusterListener;
import org.meteor.common.Node;

import java.util.List;

public interface ClusterCoordinator {
    void start() throws Exception;

    List<Node> getClusterNodes();

    List<Node> getClusterUpNodes();

    Node getClusterNode(String id);

    Node getThisNode();

    void shutdown() throws Exception;

    String getController() throws Exception;

    boolean isController();

    void addClusterListener(ClusterListener listener);

    String getClusterName();
}
