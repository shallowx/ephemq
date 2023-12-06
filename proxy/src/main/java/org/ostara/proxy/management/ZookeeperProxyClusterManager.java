package org.ostara.proxy.management;

import org.ostara.beans.CoreConfig;
import org.ostara.client.internal.ClientListener;
import org.ostara.management.ZookeeperClusterManager;

import java.util.List;

public class ZookeeperProxyClusterManager extends ZookeeperClusterManager implements ClientListener, ProxyClusterManager {
    public ZookeeperProxyClusterManager(CoreConfig config) {
        super(config);
    }

    @Override
    public List<String> route2Nodes(String key, int size) {
        return null;
    }
}
