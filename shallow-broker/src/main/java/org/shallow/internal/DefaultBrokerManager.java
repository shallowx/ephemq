package org.shallow.internal;

import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutor;
import org.shallow.ClientConfig;
import org.shallow.meta.Topic2NameserverManager;
import org.shallow.meta.TopicManager;

import java.util.Arrays;

import static org.shallow.util.ObjectUtil.isNotNull;

public class DefaultBrokerManager implements BrokerManager {

    private final BrokerConfig config;
    private final InternalClient internalClient;
    private final Topic2NameserverManager topic2NameserverManager;
    private TopicManager topicManager;
    private final ClientConfig clientConfig;

    public DefaultBrokerManager(BrokerConfig config) {
        this.config = config;
        clientConfig = new ClientConfig();
        clientConfig.setChannelPoolCapacity(config.getInternalChannelPoolLimit());
        clientConfig.setBootstrapSocketAddress(Arrays.stream(config.getNameserverUrl().split(",")).toList());
        this.internalClient = new InternalClient("broker-internal", clientConfig);

        this.topic2NameserverManager = new Topic2NameserverManager(clientConfig, this);
    }

    private EventExecutor newEventExecutor(final String name) {
        return new DefaultEventExecutor(new DefaultThreadFactory(name));
    }

    @Override
    public void start() throws Exception {
        internalClient.start();
        this.topicManager = new TopicManager(clientConfig);
    }

    @Override
    public InternalClient getInternalClient() {
        return internalClient;
    }

    @Override
    public TopicManager getTopicManager() {
        return topicManager;
    }

    @Override
    public Topic2NameserverManager getTopic2NameserverManager() {
        return topic2NameserverManager;
    }

    @Override
    public void shutdownGracefully() throws Exception {
        if (isNotNull(internalClient)) {
            internalClient.shutdownGracefully();
        }
    }

}
