package org.leopard.nameserver.metadata;

public interface Manager {

    void start() throws Exception;

    TopicWriter getTopicManager();
    ClusterWriter getClusterManager();
}
