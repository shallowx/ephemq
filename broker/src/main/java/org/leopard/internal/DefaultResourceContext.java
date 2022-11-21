package org.leopard.internal;

import org.leopard.internal.config.ServerConfig;
import org.leopard.internal.metadata.ClusterNodeCacheWriterSupport;
import org.leopard.internal.metadata.TopicPartitionRequestCacheWriterSupport;
import org.leopard.ledger.LedgerEngine;
import org.leopard.network.ChannelBoundContext;

public class DefaultResourceContext implements ResourceContext {

    private final LedgerEngine ledgerEngine;
    private final ChannelBoundContext boundContext;
    private final TopicPartitionRequestCacheWriterSupport partitionRequestCacheWriterSupport;
    private final ClusterNodeCacheWriterSupport nodeCacheWriterSupport;

    public DefaultResourceContext(ServerConfig config) throws Exception {
        this.ledgerEngine = new LedgerEngine(config);
        this.boundContext = new ChannelBoundContext();

        this.nodeCacheWriterSupport = new ClusterNodeCacheWriterSupport(config);
        this.partitionRequestCacheWriterSupport = new TopicPartitionRequestCacheWriterSupport(config, this);
    }

    @Override
    public void start() throws Exception {
        this.nodeCacheWriterSupport.start();

        this.ledgerEngine.start();
    }

    @Override
    public LedgerEngine getLedgerEngine() {
        return this.ledgerEngine;
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
    public void shutdownGracefully() throws Exception {
        this.ledgerEngine.close();
        this.nodeCacheWriterSupport.shutdown();
    }
}
