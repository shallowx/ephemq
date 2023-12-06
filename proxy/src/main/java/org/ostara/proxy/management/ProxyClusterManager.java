package org.ostara.proxy.management;

import org.ostara.management.ClusterManager;

import java.util.List;

public interface ProxyClusterManager extends ClusterManager {
    List<String> route2Nodes(String key, int size);
}
