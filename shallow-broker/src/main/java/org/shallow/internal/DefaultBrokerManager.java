package org.shallow.internal;

import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutor;
import org.shallow.MetadataAPI;
import org.shallow.MetadataProvider;
import org.shallow.cluster.ClusterMetadataProvider;
import org.shallow.topic.TopicMetadataProvider;

public class DefaultBrokerManager implements BrokerManager {

    private final BrokerConfig config;
    private final TopicMetadataProvider topicProvider;
    private final ClusterMetadataProvider clusterProvider;
    private final MetadataAPI api;

    public DefaultBrokerManager(BrokerConfig config) {
        this.config = config;
        this.api = new MetadataAPI(config.obtainMetadataWorkDirectory());
        this.topicProvider = new TopicMetadataProvider(api, newEventExecutor("topic-metadata"), config.obtainTopicsMetadataExpiredTimeMS());
        this.clusterProvider = new ClusterMetadataProvider(api, newEventExecutor("cluster-metadata"), config.obtainClustersMetadataExpiredTimeMS());
    }

    private EventExecutor newEventExecutor(final String name) {
        return new DefaultEventExecutor(new DefaultThreadFactory(name));
    }


    @Override
    public void start() throws Exception {
        api.start();
    }

    @Override
    public void shutdownGracefully() throws Exception {
        topicProvider.shutdownGracefully();
        clusterProvider.shutdownGracefully();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public MetadataProvider getTopicProvider() {
        return topicProvider;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public MetadataProvider getClusterProvider() {
        return clusterProvider;
    }
}
