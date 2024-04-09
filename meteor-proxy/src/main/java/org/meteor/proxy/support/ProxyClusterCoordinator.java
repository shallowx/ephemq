package org.meteor.proxy.support;

import java.util.Set;
import org.meteor.support.ClusterCoordinator;

public interface ProxyClusterCoordinator extends ClusterCoordinator {
    Set<String> route2Nodes(String key, int size);
}
