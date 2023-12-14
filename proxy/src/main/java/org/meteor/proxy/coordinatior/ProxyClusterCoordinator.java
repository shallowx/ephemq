package org.meteor.proxy.coordinatior;

import org.meteor.coordinatior.ClusterCoordinator;

import java.util.List;

public interface ProxyClusterCoordinator extends ClusterCoordinator {
    List<String> route2Nodes(String key, int size);
}
