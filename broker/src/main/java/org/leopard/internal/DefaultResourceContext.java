package org.leopard.internal;

import org.leopard.client.Client;
import org.leopard.client.ClientConfig;
import org.leopard.internal.config.BrokerConfig;
import org.leopard.internal.metadata.ClusterNodeCacheWriterSupport;
import org.leopard.internal.metadata.TopicPartitionRequestCacheWriterSupport;
import org.leopard.ledger.LedgerManager;
import org.leopard.network.ChannelBoundContext;

import java.util.List;

public class DefaultResourceContext implements ResourceContext {

    private final LedgerManager logManager;
    private final ChannelBoundContext boundContext;
    private final TopicPartitionRequestCacheWriterSupport partitionRequestCacheWriterSupport;
    private final ClusterNodeCacheWriterSupport nodeCacheWriterSupport;
    private final Client client;

    public DefaultResourceContext(BrokerConfig config) throws Exception {
        this.logManager = new LedgerManager(config);
        this.boundContext = new ChannelBoundContext();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setBootstrapSocketAddress(List.of(config.getNameserverUrl()));
        this.client = new Client("nameserver-client", clientConfig);

        this.nodeCacheWriterSupport = new ClusterNodeCacheWriterSupport(config, client);
        this.partitionRequestCacheWriterSupport = new TopicPartitionRequestCacheWriterSupport(config, this);
    }

    @Override
    public void start() throws Exception {
        this.client.start();
        this.nodeCacheWriterSupport.start();

        this.logManager.start();
    }

    @Override
    public LedgerManager getLedgerManager() {
        return this.logManager;
    }

    @Override
    public ChannelBoundContext getChannelBoundContext() {
        return this.boundContext;
    }

    @Override
    public TopicPartitionRequestCacheWriterSupport getPartitionRequestCacheWriterSupport() {
        return this.partitionRequestCacheWriterSupport;
    }

    @Override
    public ClusterNodeCacheWriterSupport getNodeCacheWriterSupport() {
        return this.nodeCacheWriterSupport;
    }

    @Override
    public Client getInternalClient() {
        return this.client;
    }

    @Override
    public void shutdownGracefully() throws Exception {
        this.logManager.close();
        this.nodeCacheWriterSupport.shutdown();
    }
}
