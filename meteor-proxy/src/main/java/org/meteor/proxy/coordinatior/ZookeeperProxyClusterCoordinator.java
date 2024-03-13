package org.meteor.proxy.coordinatior;

import org.apache.curator.framework.state.ConnectionState;
import org.apache.zookeeper.data.Stat;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.common.message.Node;
import org.meteor.coordinator.ZookeeperClusterCoordinator;
import org.meteor.internal.ZookeeperClientFactory;
import org.meteor.listener.ClusterListener;
import org.meteor.proxy.internal.ProxyConfig;
import org.meteor.proxy.internal.ProxyServerConfig;

import java.util.Set;

class ZookeeperProxyClusterCoordinator extends ZookeeperClusterCoordinator implements ClusterListener, ProxyClusterCoordinator {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ZookeeperProxyClusterCoordinator.class);
    private final ConsistentHashingRing hashingRing;
    private final ProxyConfig proxyConfiguration;

    public ZookeeperProxyClusterCoordinator(ProxyServerConfig configuration) {
        super(configuration);
        this.proxyConfiguration = configuration.getProxyConfiguration();
        this.client = ZookeeperClientFactory.getReadyClient(proxyConfiguration.getZookeeperConfiguration(), proxyConfiguration.getCommonConfiguration().getClusterName());
        this.hashingRing = new ConsistentHashingRing();
        this.listeners.add(this);
    }

    @Override
    public void start() throws Exception {
        connectionStateListener = (client, newState) -> {
            if (newState == ConnectionState.RECONNECTED) {
                try {
                    Stat stat = client.checkExists().forPath(String.format(ZookeeperProxyPathConstants.PROXIES_ID, proxyConfiguration.getCommonConfiguration().getServerId()));
                    if (stat != null) {
                        return;
                    }
                    registerNode(ZookeeperProxyPathConstants.PROXIES_ID);
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        };
        this.client.getConnectionStateListenable().addListener(connectionStateListener);
        startBrokersListener(ZookeeperProxyPathConstants.PROXIES_IDS);
        registerNode(ZookeeperProxyPathConstants.PROXIES_ID);
    }

    @Override
    public Set<String> route2Nodes(String key, int size) {
        return hashingRing.route2Nodes(key, size);
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

    @Override
    public void shutdown() throws Exception {
        unregistered(ZookeeperProxyPathConstants.PROXIES_ID);
        if (cache != null) {
            cache.close();
        }
        this.client.getConnectionStateListenable().removeListener(connectionStateListener);
    }
}
