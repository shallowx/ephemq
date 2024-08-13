package org.meteor.proxy.support;

import java.util.Set;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.zookeeper.data.Stat;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.common.message.Node;
import org.meteor.internal.ZookeeperClientFactory;
import org.meteor.listener.ClusterListener;
import org.meteor.proxy.core.ProxyConfig;
import org.meteor.proxy.core.ProxyServerConfig;
import org.meteor.support.ZookeeperClusterManager;

class ZookeeperProxyClusterManager extends ZookeeperClusterManager implements ClusterListener, ProxyClusterManager {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ZookeeperProxyClusterManager.class);
    private final ConsistentHashingRing hashingRing;
    private final ProxyConfig proxyConfiguration;

    public ZookeeperProxyClusterManager(ProxyServerConfig configuration) {
        super(configuration, null);
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
                        if (logger.isDebugEnabled()) {
                            logger.debug("Zookeeper proxy cluster manager has been started.");
                        }
                        return;
                    }
                    registerNode(ZookeeperProxyPathConstants.PROXIES_ID);
                } catch (Exception e) {
                    if (logger.isErrorEnabled()) {
                        logger.error(e.getMessage(), e);
                    }
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
