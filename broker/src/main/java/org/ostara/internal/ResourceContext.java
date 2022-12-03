package org.ostara.internal;

import org.ostara.internal.atomic.DistributedAtomicInteger;
import org.ostara.internal.metadata.ClusterNodeCacheWriterSupport;
import org.ostara.internal.metadata.TopicPartitionRequestCacheWriterSupport;
import org.ostara.ledger.LedgerEngine;
import org.ostara.network.ChannelBoundContext;

public interface ResourceContext {
    void start() throws Exception;

    void shutdownGracefully() throws Exception;

    LedgerEngine getLedgerEngine();

    ChannelBoundContext getChannelBoundContext();

    TopicPartitionRequestCacheWriterSupport getPartitionRequestCacheWriterSupport();

    ClusterNodeCacheWriterSupport getNodeCacheWriterSupport();

    DistributedAtomicInteger getAtomicInteger();
}
