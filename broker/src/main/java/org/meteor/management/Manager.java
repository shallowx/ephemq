package org.meteor.management;


import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import org.meteor.ledger.LogManager;
import org.meteor.listener.MetricsListener;
import org.meteor.client.internal.Client;
import org.meteor.listener.APIListener;

import java.util.List;

public interface Manager {
    void start() throws Exception;

    void shutdown() throws Exception;

    TopicManager getTopicManager();

    ClusterManager getClusterManager();

    LogManager getLogManager();

    ConnectionManager getConnectionManager();

    void addMetricsListener(MetricsListener listener);

    List<APIListener> getAPIListeners();

    EventExecutorGroup getCommandHandleEventExecutorGroup();

    EventExecutorGroup getMessageStorageEventExecutorGroup();

    EventExecutorGroup getMessageDispatchEventExecutorGroup();

    EventExecutorGroup getAuxEventExecutorGroup();

    List<EventExecutor> getAuxEventExecutors();

    Client getInnerClient();
}
