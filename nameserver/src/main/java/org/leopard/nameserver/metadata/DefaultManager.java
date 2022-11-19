package org.leopard.nameserver.metadata;

import org.leopard.NameserverConfig;

public class DefaultManager implements Manager {

    private final TopicManager topicManager;
    private final ClusterManager clusterManager;

    public DefaultManager(NameserverConfig config) {
        this.topicManager = new TopicManager();
        this.clusterManager = new ClusterManager(config);
    }

    @Override
    public void start() throws Exception {
        this.clusterManager.start();
    }

    @Override
    public TopicManager getTopicManager() {
        return this.topicManager;
    }

    @Override
    public ClusterManager getClusterManager() {
        return this.clusterManager;
    }
}
