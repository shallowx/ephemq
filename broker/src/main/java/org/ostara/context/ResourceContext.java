package org.ostara.context;

import org.ostara.atomic.DistributedAtomicInteger;
import org.ostara.ledger.LedgerEngine;
import org.ostara.network.ChannelBoundContext;

public interface ResourceContext {
    void start() throws Exception;

    void shutdownGracefully() throws Exception;

    LedgerEngine getLedgerEngine();

    ChannelBoundContext getChannelBoundContext();

    CachingTopicPartition getPartitionRequestCacheSupport();

    CachingClusterNode getNodeCacheSupport();

    DistributedAtomicInteger getAtomicInteger();
}
