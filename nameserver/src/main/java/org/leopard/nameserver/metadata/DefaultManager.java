package org.leopard.nameserver.metadata;

import org.leopard.NameserverConfig;

public class DefaultManager implements Manager {

    private final TopicWriter topicWriter;
    private final ClusterWriter clusterWriter;

    public DefaultManager(NameserverConfig config) {
        this.topicWriter = new TopicWriter();
        this.clusterWriter = new ClusterWriter(config);
    }

    @Override
    public void start() throws Exception {
        this.clusterWriter.start();
    }

    @Override
    public TopicWriter getTopicManager() {
        return this.topicWriter;
    }

    @Override
    public ClusterWriter getClusterManager() {
        return this.clusterWriter;
    }
}
