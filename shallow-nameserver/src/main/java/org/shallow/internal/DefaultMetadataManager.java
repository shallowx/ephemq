package org.shallow.internal;

import io.netty.util.concurrent.EventExecutorGroup;
import org.shallow.api.MappedFileAPI;
import org.shallow.listener.ClusterListener;
import org.shallow.listener.DefaultClusterListener;
import org.shallow.listener.DefaultTopicListener;
import org.shallow.listener.TopicListener;
import org.shallow.nraft.LeaderElector;
import org.shallow.nraft.RaftLeaderElector;
import org.shallow.provider.ClusterMetadataProvider;
import org.shallow.provider.TopicMetadataProvider;
import org.shallow.util.NetworkUtil;

import static org.shallow.util.NetworkUtil.newEventExecutorGroup;

public class DefaultMetadataManager implements MetadataManager {

    private final MetadataConfig config;
    private final MappedFileAPI api;
    private final TopicMetadataProvider topicMetadataProvider;
    private final ClusterMetadataProvider clusterMetadataProvider;
    private final EventExecutorGroup commandEventExecutorGroup;
    private final LeaderElector leaderElector;

    public DefaultMetadataManager(MetadataConfig config) {
        this.config = config;
        this.api = new MappedFileAPI(config.getWorkDirectory(), newEventExecutorGroup(1, "api-provider"), config);

        final TopicListener topicListener = new DefaultTopicListener();
        this.topicMetadataProvider = new TopicMetadataProvider(api, newEventExecutorGroup(2, "topic-provider"), topicListener);

        final ClusterListener listener = new DefaultClusterListener();
        this.clusterMetadataProvider = new ClusterMetadataProvider(config, api, newEventExecutorGroup(2, "cluster-provider"), listener);

        this.commandEventExecutorGroup = newEventExecutorGroup(1, "command");
        this.leaderElector = new RaftLeaderElector(newEventExecutorGroup(3, "cluster-raft"), config);
    }

    @Override
    public void start() throws Exception {
        api.start();

        leaderElector.start();

        topicMetadataProvider.start();
        clusterMetadataProvider.start();
    }

    @Override
    public void shutdownGracefully() throws Exception {
        clusterMetadataProvider.shutdownGracefully();
    }

    @Override
    public TopicMetadataProvider getTopicMetadataProvider() {
        return topicMetadataProvider;
    }

    @Override
    public ClusterMetadataProvider getClusterMetadataProvider() {
        return clusterMetadataProvider;
    }

    @Override
    public EventExecutorGroup commandEventExecutorGroup() {
        return commandEventExecutorGroup;
    }

    @Override
    public LeaderElector getLeaderElector() {
        return leaderElector;
    }
}
