package org.meteor.proxy.support;

import java.util.Set;
import org.meteor.support.ClusterManager;

public interface ProxyClusterManager extends ClusterManager {
    Set<String> route2Nodes(String key, int size);
}
