package org.shallow.internal;

import io.netty.util.concurrent.EventExecutorGroup;
import org.shallow.topic.TopicMetadataProvider;

public interface MetaManager {
    void start() throws Exception;
    void shutdownGracefully() throws Exception;

    TopicMetadataProvider getTopicMetadataProvider();

    EventExecutorGroup commandEventExecutorGroup();
}
