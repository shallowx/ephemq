package org.meteor.proxy.coordinator;

import org.meteor.coordinator.ClusterCoordinator;

import java.util.Set;

public interface ProxyClusterCoordinator extends ClusterCoordinator {
    Set<String> route2Nodes(String key, int size);
}
