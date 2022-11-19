package org.leopard.internal;

import org.leopard.client.Client;
import org.leopard.internal.metadata.ClusterNodeCacheWriterSupport;
import org.leopard.internal.metadata.TopicPartitionRequestCacheWriterSupport;
import org.leopard.ledger.LedgerManager;
import org.leopard.network.BrokerConnectionManager;

public interface BrokerManager {
    void start() throws Exception;
    void shutdownGracefully() throws Exception;

    LedgerManager getLedgerManager();

    BrokerConnectionManager getBrokerConnectionManager();

    TopicPartitionRequestCacheWriterSupport getTopicPartitionCache();

    ClusterNodeCacheWriterSupport getClusterCache();

     Client getInternalClient();
}
