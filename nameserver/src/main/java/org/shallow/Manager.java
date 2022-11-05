package org.shallow;

import org.shallow.metadata.ClusterManager;
import org.shallow.metadata.HeartbeatManager;
import org.shallow.metadata.TopicManager;

public interface Manager {
    TopicManager getTopicManager();
    ClusterManager getClusterManager();
    HeartbeatManager getHeartbeatManager();
}
