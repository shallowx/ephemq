package org.shallow.internal;

import org.shallow.MetadataProvider;

@SuppressWarnings("all")
public interface BrokerManager {

    void start() throws Exception;
    void shutdownGracefully() throws Exception;

    MetadataProvider getTopicProvider();
    MetadataProvider getClusterProvider();
}
