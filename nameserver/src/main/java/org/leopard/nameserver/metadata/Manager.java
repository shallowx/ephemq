package org.leopard.nameserver.metadata;

public interface Manager {

    void start() throws Exception;

    TopicManager getTopicManager();
    ClusterManager getClusterManager();
}
