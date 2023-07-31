package org.ostara.management;

import org.ostara.common.Node;
import org.ostara.listener.ClusterListener;

import java.util.List;

public interface ClusterManager {
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
