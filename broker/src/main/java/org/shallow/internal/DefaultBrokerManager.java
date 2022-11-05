package org.shallow.internal;

import org.shallow.client.Client;
import org.shallow.client.ClientConfig;
import org.shallow.internal.metadata.ClusterNodeCache;
import org.shallow.internal.metadata.TopicPartitionRequestCache;
import org.shallow.ledger.LedgerManager;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.network.BrokerConnectionManager;

import java.util.List;

public class DefaultBrokerManager implements BrokerManager {

    private final LedgerManager logManager;
    private final BrokerConnectionManager connectionManager;
    private final TopicPartitionRequestCache topicPartitionRequestCache;
    private final ClusterNodeCache clusterNodeCache;
    private final Client client;

    public DefaultBrokerManager(BrokerConfig config) throws Exception {
        this.logManager = new LedgerManager(config);
        this.connectionManager = new BrokerConnectionManager();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of(config.getNameserverUrl()));
        this.client = new Client("nameserver-client", clientConfig);

        this.topicPartitionRequestCache = new TopicPartitionRequestCache(config, this);
        this.clusterNodeCache = new ClusterNodeCache(config, client);
    }

    @Override
    public void start() throws Exception {
        this.client.start();

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
    public TopicPartitionRequestCache getTopicPartitionCache() {
        return this.topicPartitionRequestCache;
    }

    @Override
    public ClusterNodeCache getClusterCache() {
        return this.clusterNodeCache;
    }

    @Override
    public Client getInternalClient() {
        return this.client;
    }

    @Override
    public void shutdownGracefully() throws Exception {
        this.logManager.close();
    }
}
