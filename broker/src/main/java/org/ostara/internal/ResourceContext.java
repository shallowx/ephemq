package org.ostara.internal;

import org.ostara.internal.atomic.DistributedAtomicInteger;
import org.ostara.internal.metadata.CachingClusterNode;
import org.ostara.internal.metadata.CachingTopicPartition;
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
