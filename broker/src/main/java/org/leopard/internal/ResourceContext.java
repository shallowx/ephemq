package org.leopard.internal;

import org.leopard.client.Client;
import org.leopard.internal.metadata.ClusterNodeCacheWriterSupport;
import org.leopard.internal.metadata.TopicPartitionRequestCacheWriterSupport;
import org.leopard.ledger.LedgerManager;
import org.leopard.network.ChannelBoundContext;

public interface ResourceContext {
    void start() throws Exception;

    void shutdownGracefully() throws Exception;

    LedgerManager getLedgerManager();

    ChannelBoundContext getChannelBoundContext();

    TopicPartitionRequestCacheWriterSupport getPartitionRequestCacheWriterSupport();

    ClusterNodeCacheWriterSupport getNodeCacheWriterSupport();

    Client getInternalClient();
}
