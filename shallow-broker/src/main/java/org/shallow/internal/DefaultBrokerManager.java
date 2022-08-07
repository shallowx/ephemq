package org.shallow.internal;

import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutor;
import org.shallow.internal.client.Client2Broker;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.metadata.ClusterManager;
import org.shallow.metadata.TopicManager;

import java.util.Arrays;

public class DefaultBrokerManager implements BrokerManager {

    private final BrokerConfig config;
    private final Client2Broker brokerInternalClient;
    private final TopicManager topic2NameserverManager;
    private ClusterManager cluster2NameManager;
    private TopicManager topicManager;

    public DefaultBrokerManager(BrokerConfig config) {
        this.config = config;

        this.brokerInternalClient = new Client2Broker();
        this.topic2NameserverManager = new TopicManager(this);
    }

    private EventExecutor newEventExecutor(final String name) {
        return new DefaultEventExecutor(new DefaultThreadFactory(name));
    }

    @Override
    public void start() throws Exception {

        this.cluster2NameManager = new ClusterManager(this, config);
        cluster2NameManager.start();

        this.topicManager = new TopicManager(this);
    }


    @Override
    public TopicManager getTopicManager() {
        return topicManager;
    }

    @Override
    public void shutdownGracefully() throws Exception {

    }
}
