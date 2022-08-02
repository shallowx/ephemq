package org.shallow.internal;

import io.netty.util.concurrent.EventExecutorGroup;
import org.shallow.api.MappedFileAPI;
import org.shallow.provider.ClusterMetadataProvider;
import org.shallow.provider.TopicMetadataProvider;

import static org.shallow.util.NetworkUtil.newEventExecutorGroup;

public class DefaultMetadataManager implements MetadataManager {

    private final MetadataConfig config;
    private final MappedFileAPI api;
    private final TopicMetadataProvider topicMetadataProvider;
    private final ClusterMetadataProvider clusterMetadataProvider;
    private final EventExecutorGroup commandEventExecutorGroup;

    public DefaultMetadataManager(MetadataConfig config) {
        this.config = config;
        this.api = new MappedFileAPI(config.getWorkDirectory());
        this.topicMetadataProvider = new TopicMetadataProvider(api, newEventExecutorGroup(2, "topic-provider"));
        this.clusterMetadataProvider = new ClusterMetadataProvider(config, api, newEventExecutorGroup(2, "cluster-provider"));
        this.commandEventExecutorGroup = newEventExecutorGroup(1, "command");
    }

    @Override
    public void start() throws Exception {
        api.start();
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
}
