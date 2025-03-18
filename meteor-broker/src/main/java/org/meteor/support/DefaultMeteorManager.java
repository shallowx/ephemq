package org.meteor.support;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.meteor.client.core.Client;
import org.meteor.client.core.ClientConfig;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.config.ServerConfig;
import org.meteor.internal.InternalClient;
import org.meteor.internal.InternalClientListener;
import org.meteor.internal.ZookeeperClientFactory;
import org.meteor.ledger.LogHandler;
import org.meteor.listener.*;
import org.meteor.remote.util.NetworkUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * DefaultMeteorManager is a concrete implementation of the {@link Manager} interface.
 * It coordinates various subsystems such as cluster management, logging,
 * connection handling, and event execution related functionalities.
 */
public class DefaultMeteorManager implements Manager {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(DefaultMeteorManager.class);
    /**
     * A list of APIListener instances registered to handle command execution events.
     * These listeners are notified when a command is executed, allowing them to perform custom actions
     * based on the command execution details such as command code, processed bytes, execution time, and result.
     */
    private final List<APIListener> apiListeners = new ObjectArrayList<>();
    /**
     * The LogHandler instance responsible for handling logging activities
     * within the DefaultMeteorManager.
     */
    protected LogHandler logHandler;
    /**
     * Provides support for handling topics within the DefaultMeteorManager.
     * This includes functionalities such as creating, deleting, and managing topics
     * and their respective partitions, as well as handling leadership and adding listeners.
     */
    protected TopicHandleSupport support;
    /**
     * The ClusterManager responsible for managing the cluster of nodes.
     * It handles the lifecycle, state, and interactions among nodes in
     * the cluster, ensuring they work together cohesively. The ClusterManager
     * provides functionalities for starting up, shutting down, and monitoring
     * the cluster state.
     */
    protected ClusterManager clusterManager;
    /**
     * Holds the configuration settings necessary for initializing and running the server.
     * <p>
     * This variable is an instance of the ServerConfig class, which encapsulates various
     * configuration parameters such as network settings, chunk dispatch settings,
     * message handling configurations, metrics settings, and more.
     */
    protected ServerConfig configuration;
    /**
     * Manages the connection used for handling the state and readiness of communication channels.
     */
    protected Connection connection;
    /**
     * An {@link EventExecutorGroup} utilized to handle various asynchronous operations within the {@code DefaultMeteorManager}.
     * It is initialized during the creation of the manager instance and used for executing group-specific tasks.
     */
    protected EventExecutorGroup handleGroup;
    /**
     * An {@link EventExecutorGroup} responsible for handling tasks related to message storage.
     * Typically used for executing asynchronous operations within the context of message storage,
     * ensuring that storage-related tasks are properly managed and executed in a separate thread group.
     */
    protected EventExecutorGroup storageGroup;
    /**
     * The EventExecutorGroup that handles the dispatch of messages or tasks within the DefaultMeteorManager.
     * This group is responsible for managing the execution of events related to message dispatch, ensuring
     * they are handled asynchronously and efficiently.
     */
    protected EventExecutorGroup dispatchGroup;
    /**
     * The syncGroup variable in DefaultMeteorManager is an instance of EventExecutorGroup
     * used to manage a group of event executors responsible for synchronous task execution.
     * This ensures tasks are executed in a controlled, concurrent manner, maintaining
     * synchronization across various components of the system.
     */
    protected EventExecutorGroup syncGroup;
    /**
     * Auxiliary EventExecutorGroup used in the DefaultMeteorManager for handling additional tasks.
     * Provides execution capabilities for auxiliary operations that do not fall under
     * command handling, message storage, or message dispatching.
     */
    protected EventExecutorGroup auxGroup;
    /**
     *
     */
    protected List<EventExecutor> auxEventExecutors;
    /**
     * Internal client used for handling internal operations within the DefaultMeteorManager.
     * Typically utilized for communication and internal management tasks related to the
     * meteor application's infrastructure.
     */
    protected Client internalClient;

    /**
     * DefaultMeteorManager is responsible for managing various server components and their interactions.
     * It initializes the manager with default settings and prepares it to handle server operations.
     */
    public DefaultMeteorManager() {
    }

    /**
     * Constructs a DefaultMeteorManager instance with the specified server configuration.
     *
     * @param configuration the server configuration settings to initialize the manager with.
     */
    public DefaultMeteorManager(ServerConfig configuration) {
        this.configuration = configuration;
        this.connection = new DefaultConnectionContainer();
        this.syncGroup = NetworkUtil.newEventExecutorGroup(configuration.getMessageConfig().getMessageSyncThreadLimit(), "sync-group");
        this.handleGroup = NetworkUtil.newEventExecutorGroup(configuration.getCommonConfig().getCommandHandleThreadLimit(), "command-handle-group");
        this.storageGroup = NetworkUtil.newEventExecutorGroup(configuration.getMessageConfig().getMessageStorageThreadLimit(), "storage-group");
        this.dispatchGroup = NetworkUtil.newEventExecutorGroup(configuration.getMessageConfig().getMessageDispatchThreadLimit(), "dispatch-group");

        ConsistentHashingRing hashingRing = new ConsistentHashingRing();
        clusterManager = new ZookeeperClusterManager(configuration, hashingRing);
        ClusterListener clusterListener = new DefaultClusterListener(this, configuration.getNetworkConfig());
        clusterManager.addClusterListener(clusterListener);

        logHandler = new LogHandler(configuration, this);
        support = new ZookeeperTopicHandleSupport(configuration, this, hashingRing);
        TopicListener topicListener = new DefaultTopicListener(this, configuration.getCommonConfig(), configuration.getNetworkConfig());
        support.addTopicListener(topicListener);

        auxGroup = NetworkUtil.newEventExecutorGroup(configuration.getCommonConfig().getAuxThreadLimit(), "aux-group");
        auxEventExecutors = new ArrayList<>(configuration.getCommonConfig().getAuxThreadLimit());
        for (EventExecutor executor : auxGroup) {
            auxEventExecutors.add(executor);
        }
    }

    /**
     * Initializes and starts the internal client, cluster manager, topic support, and log handler components
     * using the given configuration settings.
     *
     * @throws Exception if an error occurs during the startup process
     */
    @Override
    public void start() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        List<String> clusterNodeAddress = Collections.singletonList(configuration.getCommonConfig().getAdvertisedAddress() + ":" + configuration.getCommonConfig().getAdvertisedPort());
        clientConfig.setBootstrapAddresses(clusterNodeAddress);
        clientConfig.setChannelConnectionTimeoutMilliseconds(configuration.getNetworkConfig().getConnectionTimeoutMilliseconds());
        clientConfig.setSocketEpollPrefer(true);
        clientConfig.setSocketReceiveBufferSize(524288);
        clientConfig.setSocketSendBufferSize(524288);
        internalClient = new InternalClient("internal-client", clientConfig, new InternalClientListener(this,
                configuration.getChunkRecordDispatchConfig().getChunkDispatchSyncSemaphore()),
                configuration.getCommonConfig());
        internalClient.start();

        if (clusterManager != null) {
            clusterManager.start();
            if (logger.isInfoEnabled()) {
                logger.info("Cluster manager[{}] start successfully", clusterManager.getThisNode().getCluster());
            }

        }

        if (support != null) {
            support.start();
            if (logger.isInfoEnabled()) {
                logger.info("Topic support start successfully");
            }
        }

        if (logHandler != null) {
            logHandler.start();
            if (logger.isInfoEnabled()) {
                logger.info("Ledger log handler start successfully");
            }
        }
    }

    /**
     * Shuts down the various components managed by the DefaultMeteorManager, ensuring that all resources
     * are properly cleaned up and terminated. This includes shutting down the cluster manager, topic support,
     * ledger log handler, message handle event executor group, message dispatch event executor group, storage
     * event executor group, internal client, and API listeners.
     *
     * @throws Exception if an error occurs during the shutdown process of any component.
     */
    @Override
    public void shutdown() throws Exception {
        if (clusterManager != null) {
            if (logger.isInfoEnabled()) {
                logger.info("Cluster manager[{}] will shutdown", clusterManager.getThisNode().getCluster());
            }

            clusterManager.shutdown();
        }
        if (support != null) {
            if (logger.isInfoEnabled()) {
                logger.info("Topic support will shutdown");
            }

            support.shutdown();
        }
        if (logHandler != null) {
            if (logger.isInfoEnabled()) {
                logger.info("Ledger log handler will shutdown");
            }
            logHandler.shutdown();
        }

        if (handleGroup != null) {
            if (logger.isInfoEnabled()) {
                logger.info("Message handle event executor group will shutdown");
            }
            handleGroup.shutdownGracefully();
        }

        if (dispatchGroup != null) {
            if (logger.isInfoEnabled()) {
                logger.info("Message dispatch event executor group will shutdown");
            }
            dispatchGroup.shutdownGracefully();
        }

        if (storageGroup != null) {
            if (logger.isInfoEnabled()) {
                logger.info("Storage event executor group will shutdown");
            }
            storageGroup.shutdownGracefully();
        }

        if (internalClient != null) {
            if (logger.isInfoEnabled()) {
                logger.info("Inner client will shutdown");
            }
            internalClient.close();
        }

        for (APIListener apiListener : apiListeners) {
            if (apiListener instanceof AutoCloseable closeable) {
                closeable.close();
            }
            ZookeeperClientFactory.closeClient();
        }
    }

    /**
     * Retrieves the TopicHandleSupport instance associated with this DefaultMeteorManager.
     *
     * @return the TopicHandleSupport instance for this manager.
     */
    @Override
    public TopicHandleSupport getTopicHandleSupport() {
        return support;
    }

    /**
     * Retrieves the instance of the ClusterManager associated with this DefaultMeteorManager.
     *
     * @return the ClusterManager instance responsible for managing the cluster of nodes.
     */
    @Override
    public ClusterManager getClusterManager() {
        return clusterManager;
    }

    /**
     * Provides access to the LogHandler associated with the DefaultMeteorManager instance.
     *
     * @return the LogHandler instance used for logging operations.
     */
    @Override
    public LogHandler getLogHandler() {
        return logHandler;
    }

    /**
     * Retrieves the current connection instance managed by the DefaultMeteorManager.
     *
     * @return the active connection instance
     */
    @Override
    public Connection getConnection() {
        return connection;
    }

    /**
     * Adds a MetricsListener to the log handler, support system, and the list of API listeners.
     *
     * @param listener the MetricsListener to be added.
     */
    @Override
    public void addMetricsListener(MetricsListener listener) {
        if (logHandler != null) {
            logHandler.addLogListener(Collections.singletonList(listener));
        }

        if (support != null) {
            support.addTopicListener(listener);
        }

        apiListeners.add(listener);
    }

    /**
     * Retrieves a list of registered API listeners.
     *
     * @return a list of APIListener instances that have been added to listen for API events.
     */
    @Override
    public List<APIListener> getAPIListeners() {
        return apiListeners;
    }

    /**
     * Retrieves the {@link EventExecutorGroup} responsible for handling command-related events.
     *
     * @return the {@link EventExecutorGroup} that manages command execution events.
     */
    @Override
    public EventExecutorGroup getCommandHandleEventExecutorGroup() {
        return handleGroup;
    }

    /**
     * Retrieves the EventExecutorGroup responsible for handling message storage events.
     *
     * @return the EventExecutorGroup used for message storage operations.
     */
    @Override
    public EventExecutorGroup getMessageStorageEventExecutorGroup() {
        return storageGroup;
    }

    /**
     * Retrieves the EventExecutorGroup responsible for handling message dispatch events.
     *
     * @return the EventExecutorGroup that manages message dispatch operations.
     */
    @Override
    public EventExecutorGroup getMessageDispatchEventExecutorGroup() {
        return dispatchGroup;
    }

    /**
     * Retrieves the auxiliary EventExecutorGroup.
     *
     * @return the auxiliary EventExecutorGroup instance.
     */
    @Override
    public EventExecutorGroup getAuxEventExecutorGroup() {
        return auxGroup;
    }

    /**
     * Retrieves the list of auxiliary EventExecutor instances.
     *
     * @return a list of EventExecutor objects used for auxiliary event handling.
     */
    @Override
    public List<EventExecutor> getAuxEventExecutors() {
        return auxEventExecutors;
    }

    /**
     * Retrieves the internal client used by the DefaultMeteorManager.
     *
     * @return the internal Client instance.
     */
    @Override
    public Client getInternalClient() {
        return internalClient;
    }
}
