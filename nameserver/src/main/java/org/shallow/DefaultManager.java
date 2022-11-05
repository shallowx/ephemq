package org.shallow;

import org.shallow.metadata.ClusterManager;
import org.shallow.metadata.HeartbeatManager;
import org.shallow.metadata.TopicManager;

public class DefaultManager implements Manager {

    private final TopicManager topicManager;
    private final ClusterManager clusterManager;
    private final HeartbeatManager heartbeatManager;

    public DefaultManager() {
        this.topicManager = new TopicManager(this);
        this.clusterManager = new ClusterManager(this);
        this.heartbeatManager = new HeartbeatManager(this);
    }

    @Override
    public TopicManager getTopicManager() {
        return this.topicManager;
    }

    @Override
    public ClusterManager getClusterManager() {
        return this.clusterManager;
    }

    @Override
    public HeartbeatManager getHeartbeatManager() {
        return this.heartbeatManager;
    }
}
