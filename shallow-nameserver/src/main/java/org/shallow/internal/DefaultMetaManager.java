package org.shallow.internal;

import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutorGroup;
import org.shallow.api.MetaMappedFileAPI;
import org.shallow.cluster.ClusterMetadataProvider;
import org.shallow.topic.TopicMetadataProvider;
import org.shallow.util.NetworkUtil;

public class DefaultMetaManager implements MetaManager{

    private final MetaConfig config;
    private final MetaMappedFileAPI api;
    private final TopicMetadataProvider topicMetadataProvider;
    private final ClusterMetadataProvider clusterMetadataProvider;
    private final EventExecutorGroup commandEventExecutorGroup;

    public DefaultMetaManager(MetaConfig config) {
        this.config = config;
        this.api = new MetaMappedFileAPI(config.getWorkDirectory());
        this.topicMetadataProvider = new TopicMetadataProvider(api,
                new DefaultEventExecutor(new DefaultThreadFactory("topic-cache-provider")),
                new DefaultEventExecutor(new DefaultThreadFactory("topic-file-provider")),
                0);

        this.clusterMetadataProvider = new ClusterMetadataProvider(api,
                new DefaultEventExecutor(new DefaultThreadFactory("cluster-cache-provider")),
                new DefaultEventExecutor(new DefaultThreadFactory("cluster-file-provider")),
                0);

        this.commandEventExecutorGroup = NetworkUtil.newEventExecutorGroup(1, "command");
    }

    @Override
    public void start() throws Exception {
        api.start();
    }

    @Override
    public void shutdownGracefully() throws Exception {

    }

    @Override
    public TopicMetadataProvider getTopicMetadataProvider() {
        return topicMetadataProvider;
    }

    public ClusterMetadataProvider getClusterMetadataProvider() {
        return clusterMetadataProvider;
    }

    @Override
    public EventExecutorGroup commandEventExecutorGroup() {
        return commandEventExecutorGroup;
    }
}
