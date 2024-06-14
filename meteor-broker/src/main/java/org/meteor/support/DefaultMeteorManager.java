package org.meteor.support;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import org.meteor.client.core.Client;
import org.meteor.client.core.ClientConfig;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.config.ServerConfig;
import org.meteor.internal.InternalClient;
import org.meteor.internal.InternalClientListener;
import org.meteor.internal.ZookeeperClientFactory;
import org.meteor.ledger.LogHandler;
import org.meteor.listener.APIListener;
import org.meteor.listener.ClusterListener;
import org.meteor.listener.DefaultClusterListener;
import org.meteor.listener.DefaultTopicListener;
import org.meteor.listener.MetricsListener;
import org.meteor.listener.TopicListener;
import org.meteor.remote.util.NetworkUtil;

public class DefaultMeteorManager implements Manager {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(DefaultMeteorManager.class);
    private final List<APIListener> apiListeners = new LinkedList<>();
    protected LogHandler logHandler;
    protected TopicHandleSupport support;
    protected ClusterManager clusterManager;
    protected ServerConfig configuration;
    protected Connection connection;
    protected EventExecutorGroup handleGroup;
    protected EventExecutorGroup storageGroup;
    protected EventExecutorGroup dispatchGroup;
    protected EventExecutorGroup syncGroup;
    protected EventExecutorGroup auxGroup;
    protected List<EventExecutor> auxEventExecutors;
    protected Client internalClient;

    public DefaultMeteorManager() {
    }

    public DefaultMeteorManager(ServerConfig configuration) {
        this.configuration = configuration;
        this.connection = new DefaultConnectionArraySet();
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
            if (apiListener instanceof AutoCloseable) {
                ((AutoCloseable) apiListener).close();
            }
            ZookeeperClientFactory.closeClient();
        }
    }

    @Override
    public TopicHandleSupport getTopicHandleSupport() {
        return support;
    }

    @Override
    public ClusterManager getClusterManager() {
        return clusterManager;
    }

    @Override
    public LogHandler getLogHandler() {
        return logHandler;
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

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

    @Override
    public List<APIListener> getAPIListeners() {
        return apiListeners;
    }

    @Override
    public EventExecutorGroup getCommandHandleEventExecutorGroup() {
        return handleGroup;
    }

    @Override
    public EventExecutorGroup getMessageStorageEventExecutorGroup() {
        return storageGroup;
    }

    @Override
    public EventExecutorGroup getMessageDispatchEventExecutorGroup() {
        return dispatchGroup;
    }

    @Override
    public EventExecutorGroup getAuxEventExecutorGroup() {
        return auxGroup;
    }

    @Override
    public List<EventExecutor> getAuxEventExecutors() {
        return auxEventExecutors;
    }

    @Override
    public Client getInternalClient() {
        return internalClient;
    }
}
