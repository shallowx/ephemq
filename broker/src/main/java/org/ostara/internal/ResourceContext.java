package org.ostara.internal;

import org.ostara.internal.atomic.DistributedAtomicInteger;
import org.ostara.internal.metadata.ClusterNodeCacheSupport;
import org.ostara.internal.metadata.TopicPartitionRequestCacheSupport;
import org.ostara.ledger.LedgerEngine;
import org.ostara.network.ChannelBoundContext;

public interface ResourceContext {
    void start() throws Exception;

    void shutdownGracefully() throws Exception;

    LedgerEngine getLedgerEngine();

    ChannelBoundContext getChannelBoundContext();

    TopicPartitionRequestCacheSupport getPartitionRequestCacheSupport();

    ClusterNodeCacheSupport getNodeCacheSupport();

    DistributedAtomicInteger getAtomicInteger();
}
