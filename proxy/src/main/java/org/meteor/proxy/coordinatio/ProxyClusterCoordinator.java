package org.meteor.proxy.coordinatio;

import org.meteor.coordinatio.ClusterCoordinator;

import java.util.List;

public interface ProxyClusterCoordinator extends ClusterCoordinator {
    List<String> route2Nodes(String key, int size);
}
