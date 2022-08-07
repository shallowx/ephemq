package org.shallow.internal;

import org.shallow.metadata.TopicManager;

@SuppressWarnings("all")
public interface BrokerManager {
    void start() throws Exception;
    void shutdownGracefully() throws Exception;
    TopicManager getTopicManager();
}
