package org.shallow.internal;

import org.shallow.client.Client;
import org.shallow.internal.metadata.ClusterNodeCacheSupport;
import org.shallow.internal.metadata.TopicPartitionRequestCacheSupport;
import org.shallow.ledger.LedgerManager;
import org.shallow.network.BrokerConnectionManager;

public interface BrokerManager {
    void start() throws Exception;
    void shutdownGracefully() throws Exception;

    LedgerManager getLedgerManager();

    BrokerConnectionManager getBrokerConnectionManager();

    TopicPartitionRequestCacheSupport getTopicPartitionCache();

    ClusterNodeCacheSupport getClusterCache();

     Client getInternalClient();
}
