package org.shallow.internal;

import org.shallow.client.Client;
import org.shallow.internal.metadata.ClusterNodeCache;
import org.shallow.internal.metadata.TopicPartitionRequestCache;
import org.shallow.ledger.LedgerManager;
import org.shallow.network.BrokerConnectionManager;

public interface BrokerManager {
    void start() throws Exception;
    void shutdownGracefully() throws Exception;

    LedgerManager getLedgerManager();

    BrokerConnectionManager getBrokerConnectionManager();

    TopicPartitionRequestCache getTopicPartitionCache();

    ClusterNodeCache getClusterCache();

     Client getInternalClient();
}
