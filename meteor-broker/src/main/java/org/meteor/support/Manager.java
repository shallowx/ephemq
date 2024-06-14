package org.meteor.support;


import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import java.util.List;
import org.meteor.client.core.Client;
import org.meteor.ledger.LogHandler;
import org.meteor.listener.APIListener;
import org.meteor.listener.MetricsListener;

public interface Manager {
    void start() throws Exception;
    void shutdown() throws Exception;

    TopicHandleSupport getTopicHandleSupport();
    ClusterManager getClusterManager();
    LogHandler getLogHandler();
    Connection getConnection();
    void addMetricsListener(MetricsListener listener);
    List<APIListener> getAPIListeners();
    EventExecutorGroup getCommandHandleEventExecutorGroup();
    EventExecutorGroup getMessageStorageEventExecutorGroup();
    EventExecutorGroup getMessageDispatchEventExecutorGroup();
    EventExecutorGroup getAuxEventExecutorGroup();
    List<EventExecutor> getAuxEventExecutors();
    Client getInternalClient();
}
