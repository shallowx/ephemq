package org.meteor.proxy.management;

import org.apache.curator.framework.state.ConnectionState;
import org.apache.zookeeper.data.Stat;
import org.meteor.common.Node;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.common.util.ConsistentHashingRing;
import org.meteor.configuration.ProxyConfiguration;
import org.meteor.configuration.ServerConfiguration;
import org.meteor.listener.ClusterListener;
import org.meteor.management.ZookeeperClient;
import org.meteor.management.ZookeeperClusterManager;
import java.util.List;

public class ZookeeperProxyClusterManager extends ZookeeperClusterManager implements ClusterListener, ProxyClusterManager {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ZookeeperProxyClusterManager.class);
    private final ConsistentHashingRing hashingRing;
    private final ProxyConfiguration proxyConfiguration;
    public ZookeeperProxyClusterManager(ServerConfiguration configuration) {
        super(configuration);
        this.proxyConfiguration = configuration.getProxyConfiguration();
        this.client = ZookeeperClient.getClient(proxyConfiguration.getZookeeperConfiguration(), proxyConfiguration.getCommonConfiguration().getClusterName());
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
                  Stat stat = client.checkExists().forPath(String.format(ZookeeperPathConstants.PROXIES_ID, proxyConfiguration.getCommonConfiguration().getServerId()));
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
        startBrokersListener(ZookeeperPathConstants.PROXIES_IDS);
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
