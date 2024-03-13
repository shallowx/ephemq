package org.meteor.coordinator;


import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import org.meteor.client.internal.Client;
import org.meteor.ledger.LogHandler;
import org.meteor.listener.APIListener;
import org.meteor.listener.MetricsListener;

import java.util.List;

public interface Coordinator {
    void start() throws Exception;

    void shutdown() throws Exception;

    TopicCoordinator getTopicCoordinator();

    ClusterCoordinator getClusterCoordinator();

    LogHandler getLogCoordinator();

    ConnectionCoordinator getConnectionCoordinator();

    void addMetricsListener(MetricsListener listener);

    List<APIListener> getAPIListeners();

    EventExecutorGroup getCommandHandleEventExecutorGroup();

    EventExecutorGroup getMessageStorageEventExecutorGroup();

    EventExecutorGroup getMessageDispatchEventExecutorGroup();

    EventExecutorGroup getAuxEventExecutorGroup();

    List<EventExecutor> getAuxEventExecutors();

    Client getInternalClient();
}
