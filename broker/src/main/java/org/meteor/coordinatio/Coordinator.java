package org.meteor.coordinatio;


import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import org.meteor.ledger.LogManager;
import org.meteor.listener.MetricsListener;
import org.meteor.client.internal.Client;
import org.meteor.listener.APIListener;

import java.util.List;

public interface Coordinator {
    void start() throws Exception;

    void shutdown() throws Exception;

    TopicCoordinator getTopicManager();

    ClusterCoordinator getClusterManager();

    LogManager getLogManager();

    ConnectionCoordinator getConnectionManager();

    void addMetricsListener(MetricsListener listener);

    List<APIListener> getAPIListeners();

    EventExecutorGroup getCommandHandleEventExecutorGroup();

    EventExecutorGroup getMessageStorageEventExecutorGroup();

    EventExecutorGroup getMessageDispatchEventExecutorGroup();

    EventExecutorGroup getAuxEventExecutorGroup();

    List<EventExecutor> getAuxEventExecutors();

    Client getInnerClient();
}
