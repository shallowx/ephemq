package org.ostara.internal;

import org.ostara.internal.atomic.DistributedAtomicInteger;
import org.ostara.internal.config.ServerConfig;
import org.ostara.internal.metadata.ClusterNodeCacheSupport;
import org.ostara.internal.metadata.TopicPartitionRequestCacheSupport;
import org.ostara.ledger.LedgerEngine;
import org.ostara.network.ChannelBoundContext;

public class DefaultResourceContext implements ResourceContext {

    private final LedgerEngine ledgerEngine;
    private final ChannelBoundContext boundContext;
    private final TopicPartitionRequestCacheSupport partitionRequestCacheWriterSupport;
    private final ClusterNodeCacheSupport nodeCacheWriterSupport;
    private final DistributedAtomicInteger distributedAtomicInteger;

    public DefaultResourceContext(ServerConfig config) throws Exception {
        this.ledgerEngine = new LedgerEngine(config);
        this.boundContext = new ChannelBoundContext();

        this.distributedAtomicInteger = new DistributedAtomicInteger();
        this.nodeCacheWriterSupport = new ClusterNodeCacheSupport(config);
        this.partitionRequestCacheWriterSupport = new TopicPartitionRequestCacheSupport(config, this);
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
    public TopicPartitionRequestCacheSupport getPartitionRequestCacheSupport() {
        return this.partitionRequestCacheWriterSupport;
    }

    @Override
    public DistributedAtomicInteger getAtomicInteger() {
        return distributedAtomicInteger;
    }

    @Override
    public ClusterNodeCacheSupport getNodeCacheSupport() {
        return this.nodeCacheWriterSupport;
    }

    @Override
    public void shutdownGracefully() throws Exception {
        this.ledgerEngine.close();
        this.nodeCacheWriterSupport.shutdown();
    }
}
