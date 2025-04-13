package org.meteor.support;


import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import java.util.List;
import org.meteor.client.core.Client;
import org.meteor.ledger.LogHandler;
import org.meteor.listener.APIListener;
import org.meteor.listener.MetricsListener;
import org.meteor.zookeeper.ClusterManager;
import org.meteor.zookeeper.TopicHandleSupport;

/**
 * The Manager interface represents a managerial contract for handling various server components and their interactions.
 * It defines methods for starting and shutting down operations, and provides access to various supporting components
 * such as topic handles, cluster management, logging, connections, and event executors.
 */
public interface Manager {
    /**
     * Starts the Manager instance, initializing and starting its various server components and interactions as defined
     * by the specific implementation. This method may include tasks such as starting internal clients, cluster managers,
     * topic support systems, log handlers, and other necessary components for operations.
     *
     * @throws Exception if an error occurs during the startup process
     */
    void start() throws Exception;

    /**
     * Shuts down the server and its associated resources, ensuring all components are properly terminated.
     *
     * @throws Exception if an error occurs during the shutdown process.
     */
    void shutdown() throws Exception;

    /**
     * Provides access to the TopicHandleSupport, which is useful for managing topic-related
     * operations such as creating, deleting, and configuring topics and partitions.
     *
     * @return the TopicHandleSupport instance to manage topics and partitions.
     */
    TopicHandleSupport getTopicHandleSupport();

    /**
     * Retrieves the ClusterManager instance responsible for managing the cluster of nodes.
     *
     * @return the ClusterManager instance that handles cluster operations, node interactions, and lifecycle management.
     */
    ClusterManager getClusterManager();

    /**
     * Retrieves the LogHandler instance associated with the Manager.
     *
     * @return the LogHandler instance responsible for handling logging operations.
     */
    LogHandler getLogHandler();

    /**
     * Retrieves the connection manager.
     *
     * @return the connection manager responsible for managing channels.
     */
    Connection getConnection();

    /**
     * Adds a listener for metrics events.
     *
     * @param listener the MetricsListener to be added. This listener will be notified of metrics-related events.
     */
    void addMetricsListener(MetricsListener listener);

    /**
     * Retrieves a list of registered APIListener instances.
     * These listeners are notified of command execution events.
     *
     * @return a List of APIListener objects.
     */
    List<APIListener> getAPIListeners();

    /**
     * Retrieves the EventExecutorGroup responsible for handling command executions.
     *
     * @return An EventExecutorGroup used for managing command handling tasks.
     */
    EventExecutorGroup getCommandHandleEventExecutorGroup();

    /**
     * Provides the EventExecutorGroup allocated for managing message storage events.
     *
     * @return the EventExecutorGroup dedicated to handling message storage related tasks.
     */
    EventExecutorGroup getMessageStorageEventExecutorGroup();

    /**
     * Retrieves the EventExecutorGroup responsible for handling message dispatch events.
     *
     * @return The EventExecutorGroup designated for message dispatch operations.
     */
    EventExecutorGroup getMessageDispatchEventExecutorGroup();

    /**
     * Retrieves the auxiliary event executor group, which is responsible for managing auxiliary tasks
     * that require asynchronous execution.
     *
     * @return An instance of {@link EventExecutorGroup} that handles auxiliary event execution tasks.
     */
    EventExecutorGroup getAuxEventExecutorGroup();

    /**
     * Retrieves a list of auxiliary event executors.
     *
     * @return a list containing the auxiliary event executors used for handling various events.
     */
    List<EventExecutor> getAuxEventExecutors();

    /**
     * Retrieves the internal client instance utilized by the manager for various operations.
     *
     * @return the internal Client instance associated with the manager.
     */
    Client getInternalClient();
}
