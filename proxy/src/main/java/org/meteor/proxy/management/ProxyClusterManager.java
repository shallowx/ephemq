package org.meteor.proxy.management;

import org.meteor.management.ClusterManager;

import java.util.List;

public interface ProxyClusterManager extends ClusterManager {
    List<String> route2Nodes(String key, int size);
}
