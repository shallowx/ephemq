package org.shallow.nameserver.metadata;

public interface Manager {

    void start() throws Exception;

    TopicManager getTopicManager();
    ClusterManager getClusterManager();
}
