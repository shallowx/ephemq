package org.leopard.internal;

import org.leopard.client.Client;
import org.leopard.internal.metadata.ClusterNodeCacheSupport;
import org.leopard.internal.metadata.TopicPartitionRequestCacheSupport;
import org.leopard.ledger.LedgerManager;
import org.leopard.network.BrokerConnectionManager;

public interface BrokerManager {
    void start() throws Exception;
    void shutdownGracefully() throws Exception;

    LedgerManager getLedgerManager();

    BrokerConnectionManager getBrokerConnectionManager();

    TopicPartitionRequestCacheSupport getTopicPartitionCache();

    ClusterNodeCacheSupport getClusterCache();

     Client getInternalClient();
}
