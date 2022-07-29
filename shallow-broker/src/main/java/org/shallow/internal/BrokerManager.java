package org.shallow.internal;

import org.shallow.meta.Topic2NameserverManager;
import org.shallow.meta.TopicManager;

@SuppressWarnings("all")
public interface BrokerManager {

    void start() throws Exception;
    void shutdownGracefully() throws Exception;

    InternalClient getInternalClient();

    TopicManager getTopicManager();

    Topic2NameserverManager getTopic2NameserverManager();
}
