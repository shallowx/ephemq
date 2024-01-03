package org.meteor.proxy.coordinatior;

import org.meteor.coordinatior.ClusterCoordinator;

import java.util.List;
import java.util.Set;

public interface ProxyClusterCoordinator extends ClusterCoordinator {
    Set<String> route2Nodes(String key, int size);
}
