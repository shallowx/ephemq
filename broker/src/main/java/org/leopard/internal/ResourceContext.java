package org.leopard.internal;

import org.leopard.internal.metadata.ClusterNodeCacheWriterSupport;
import org.leopard.internal.metadata.TopicPartitionRequestCacheWriterSupport;
import org.leopard.ledger.LedgerEngine;
import org.leopard.network.ChannelBoundContext;

public interface ResourceContext {
    void start() throws Exception;

    void shutdownGracefully() throws Exception;

    LedgerEngine getLedgerEngine();

    ChannelBoundContext getChannelBoundContext();

    TopicPartitionRequestCacheWriterSupport getPartitionRequestCacheWriterSupport();

    ClusterNodeCacheWriterSupport getNodeCacheWriterSupport();
}
