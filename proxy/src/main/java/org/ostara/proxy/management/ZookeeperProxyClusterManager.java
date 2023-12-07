package org.ostara.proxy.management;

import org.apache.curator.framework.state.ConnectionState;
import org.apache.zookeeper.data.Stat;
import org.ostara.beans.CoreConfig;
import org.ostara.common.Node;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.common.util.ConsistentHashingRing;
import org.ostara.listener.ClusterListener;
import org.ostara.management.ZookeeperClusterManager;
import java.util.List;

public class ZookeeperProxyClusterManager extends ZookeeperClusterManager implements ClusterListener, ProxyClusterManager {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ZookeeperProxyClusterManager.class);
    private final LedgerSyncManager syncManager;
    private final ConsistentHashingRing hashingRing;
    public ZookeeperProxyClusterManager(CoreConfig config, LedgerSyncManager syncManager) {
        super(config);
        this.syncManager = syncManager;
        this.hashingRing = new ConsistentHashingRing();
        this.listeners.add(this);
    }

    @Override
    public List<String> route2Nodes(String key, int size) {
        return hashingRing.route2Nodes(key,size);
    }

    @Override
    public void start() throws Exception {
        connectionStateListener = (client, newState) -> {
          if (newState == ConnectionState.RECONNECTED) {
              try {
                  Stat stat = client.checkExists().forPath(String.format(ZookeeperPathConstants.PROXIES_ID, config.getServerId()));
                  if (stat != null) {
                      return;
                  }
                  registerNode(ZookeeperPathConstants.PROXIES_ID);
              } catch (Exception e) {
                  logger.error(e.getMessage(), e);
              }
          }
        };
        this.client.getConnectionStateListenable().addListener(connectionStateListener);
        startBrokersListener(ZookeeperPathConstants.PROXIES_ID);
        registerNode(ZookeeperPathConstants.PROXIES_ID);
    }

    @Override
    public void shutdown() throws Exception {
        unregistered(ZookeeperPathConstants.PROXIES_ID);
        if (cache != null) {
            cache.close();
        }

        this.client.getConnectionStateListenable().removeListener(connectionStateListener);
    }

    @Override
    public boolean isController() {
        throw new UnsupportedOperationException();
    }


    @Override
    public void onGetControlRole(Node node) {

    }

    @Override
    public void onLostControlRole(Node node) {

    }

    @Override
    public void onNodeJoin(Node node) {
        if (node != null) {
            hashingRing.insertNode(node.getId());
        }
    }

    @Override
    public void onNodeDown(Node node) {
        if (node != null) {
            hashingRing.deleteNode(node.getId());
        }
    }

    @Override
    public void onNodeLeave(Node node) {
        if (node != null) {
            hashingRing.deleteNode(node.getId());
        }
    }
}
