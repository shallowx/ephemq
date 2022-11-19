package org.leopard.internal;

import org.leopard.client.Client;
import org.leopard.client.ClientConfig;
import org.leopard.internal.metadata.ClusterNodeCacheWriterSupport;
import org.leopard.internal.metadata.TopicPartitionRequestCacheWriterSupport;
import org.leopard.ledger.LedgerManager;
import org.leopard.internal.config.BrokerConfig;
import org.leopard.network.BrokerConnectionManager;

import java.util.List;

public class DefaultBrokerManager implements BrokerManager {

    private final LedgerManager logManager;
    private final BrokerConnectionManager connectionManager;
    private final TopicPartitionRequestCacheWriterSupport topicPartitionRequestCache;
    private final ClusterNodeCacheWriterSupport clusterNodeCache;
    private final Client client;

    public DefaultBrokerManager(BrokerConfig config) throws Exception {
        this.logManager = new LedgerManager(config);
        this.connectionManager = new BrokerConnectionManager();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of(config.getNameserverUrl()));
        this.client = new Client("nameserver-client", clientConfig);

        this.clusterNodeCache = new ClusterNodeCacheWriterSupport(config, client);
        this.topicPartitionRequestCache = new TopicPartitionRequestCacheWriterSupport(config, this);
    }

    @Override
    public void start() throws Exception {
        this.client.start();
        this.clusterNodeCache.start();

        this.logManager.start();
    }

    @Override
    public LedgerManager getLedgerManager() {
        return this.logManager;
    }

    @Override
    public BrokerConnectionManager getBrokerConnectionManager() {
        return this.connectionManager;
    }

    @Override
    public TopicPartitionRequestCacheWriterSupport getTopicPartitionCache() {
        return this.topicPartitionRequestCache;
    }

    @Override
    public ClusterNodeCacheWriterSupport getClusterCache() {
        return this.clusterNodeCache;
    }

    @Override
    public Client getInternalClient() {
        return this.client;
    }

    @Override
    public void shutdownGracefully() throws Exception {
        this.logManager.close();
        this.clusterNodeCache.shutdown();
    }
}
