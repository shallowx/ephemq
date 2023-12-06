package org.ostara.management;


import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import org.ostara.client.internal.Client;
import org.ostara.listener.APIListener;
import org.ostara.listener.MetricsListener;
import org.ostara.ledger.LogManager;

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
