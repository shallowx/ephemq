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
        ClientConfig quorumVoterClientConfig = new ClientConfig();

        quorumVoterClientConfig.setChannelFixedPoolCapacity(config.getInternalChannelPoolLimit());
        quorumVoterClientConfig.setInvokeExpiredMs(config.getInvokeTimeMs());

        this.logManager = new LedgerManager(config);
        this.connectionManager = new BrokerConnectionManager();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of(config.getNameserverUrl()));
        this.client = new Client("nameserver-client", clientConfig);

        this.topicPartitionRequestCache = new TopicPartitionRequestCache(config, client);
        this.clusterNodeCache = new ClusterNodeCache(config, client);
    }

    @Override
    public void start() throws Exception {
        logManager.start();
    }

    @Override
    public LedgerManager getLedgerManager() {
        return logManager;
    }

    @Override
    public BrokerConnectionManager getBrokerConnectionManager() {
        return connectionManager;
    }

    @Override
    public TopicPartitionRequestCache getTopicPartitionCache() {
        return topicPartitionRequestCache;
    }

    @Override
    public ClusterNodeCache getClusterCache() {
        return clusterNodeCache;
    }

    @Override
    public Client getInternalClient() {
        return client;
    }

    @Override
    public void shutdownGracefully() throws Exception {
        logManager.close();
    }
}
