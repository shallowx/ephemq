package org.shallow.internal;

import org.shallow.metadata.Cluster2NameManager;
import org.shallow.metadata.Topic2NameserverManager;
import org.shallow.meta.TopicManager;

@SuppressWarnings("all")
public interface BrokerManager {
    void start() throws Exception;
    void shutdownGracefully() throws Exception;
    NameserverInternalClient getInternalClient();
    TopicManager getTopicManager();
    Topic2NameserverManager getTopic2NameserverManager();
    Cluster2NameManager getCluster2NameManager();
}
