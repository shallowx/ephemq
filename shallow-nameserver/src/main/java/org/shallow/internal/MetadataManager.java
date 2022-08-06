package org.shallow.internal;

import io.netty.util.concurrent.EventExecutorGroup;
import org.shallow.nraft.LeaderElector;
import org.shallow.provider.ClusterMetadataProvider;
import org.shallow.provider.TopicMetadataProvider;

public interface MetadataManager {
    void start() throws Exception;
    void shutdownGracefully() throws Exception;

    TopicMetadataProvider getTopicMetadataProvider();

    ClusterMetadataProvider getClusterMetadataProvider();
    EventExecutorGroup commandEventExecutorGroup();

    LeaderElector getLeaderElector();
}
