package org.meteor.coordinatior;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import org.meteor.config.ServerConfig;
import org.meteor.internal.ZookeeperClient;
import org.meteor.ledger.LogCoordinator;
import org.meteor.listener.*;
import org.meteor.client.internal.Client;
import org.meteor.client.internal.ClientConfig;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.internal.InternalClient;
import org.meteor.internal.InternalClientListener;
import org.meteor.remote.util.NetworkUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class DefaultCoordinator implements Coordinator {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(DefaultCoordinator.class);
    private final List<APIListener> apiListeners = new LinkedList<>();
    protected LogCoordinator logCoordinator;
    protected TopicCoordinator topicCoordinator;
    protected ClusterCoordinator clusterCoordinator;
    protected ServerConfig configuration;
    protected ConnectionCoordinator connectionCoordinator;
    protected EventExecutorGroup handleGroup;
    protected EventExecutorGroup storageGroup;
    protected EventExecutorGroup dispatchGroup;
    protected EventExecutorGroup syncGroup;
    protected EventExecutorGroup auxGroup;
    protected List<EventExecutor> auxEventExecutors;
    protected Client innerClient;

    public DefaultCoordinator() {
    }

    public DefaultCoordinator(ServerConfig configuration) {
        this.configuration = configuration;
        this.connectionCoordinator = new DefaultConnectionCoordinator();
        this.syncGroup = NetworkUtil.newEventExecutorGroup(configuration.getMessageConfig().getMessageSyncThreadLimit(), "sync-group");
        this.handleGroup = NetworkUtil.newEventExecutorGroup(configuration.getCommonConfig().getCommandHandleThreadLimit(), "command-handle-group");
        this.storageGroup = NetworkUtil.newEventExecutorGroup(configuration.getMessageConfig().getMessageStorageThreadLimit(), "storage-group");
        this.dispatchGroup = NetworkUtil.newEventExecutorGroup(configuration.getMessageConfig().getMessageDispatchThreadLimit(), "dispatch-group");

        clusterCoordinator = new ZookeeperClusterCoordinator(configuration);
        ClusterListener clusterListener = new DefaultClusterListener(this, configuration.getNetworkConfig());
        clusterCoordinator.addClusterListener(clusterListener);

        logCoordinator = new LogCoordinator(configuration, this);
        topicCoordinator = new ZookeeperTopicCoordinator(configuration, this);
        TopicListener topicListener = new DefaultTopicListener(this, configuration.getCommonConfig(),configuration.getNetworkConfig());
        topicCoordinator.addTopicListener(topicListener);

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
        clientConfig.setChannelConnectionTimeoutMs(configuration.getNetworkConfig().getConnectionTimeoutMs());
        clientConfig.setSocketEpollPrefer(true);
        clientConfig.setSocketReceiveBufferSize(524288);
        clientConfig.setSocketSendBufferSize(524288);
        innerClient = new InternalClient("inner-client", clientConfig, new InternalClientListener(this, configuration.getChunkRecordDispatchConfig().getChunkDispatchSyncSemaphore()), configuration.getCommonConfig(), this);
        innerClient.start();

        if (clusterCoordinator != null) {
            clusterCoordinator.start();
            if (logger.isInfoEnabled()) {
                logger.info("Cluster coordinator<{}> start successfully", clusterCoordinator.getThisNode().getCluster());
            }

        }

        if (topicCoordinator != null) {
            if (logger.isInfoEnabled()) {
                logger.info("Topic coordinator start successfully");
            }

            topicCoordinator.start();
        }

        if (logCoordinator != null) {
            if (logger.isInfoEnabled()) {
                logger.info("Ledger log coordinator start successfully");
            }
            logCoordinator.start();
        }
    }

    @Override
    public void shutdown() throws Exception {
        if (clusterCoordinator != null) {
            if (logger.isInfoEnabled()) {
                logger.info("Cluster coordinator<{}> will shutdown", clusterCoordinator.getThisNode().getCluster());
            }

            clusterCoordinator.shutdown();
        }
        if (topicCoordinator != null) {
            if (logger.isInfoEnabled()) {
                logger.info("Topic coordinator will shutdown");
            }

            topicCoordinator.shutdown();
        }
        if (logCoordinator != null) {
            if (logger.isInfoEnabled()) {
                logger.info("Ledger log coordinator will shutdown");
            }
            logCoordinator.shutdown();
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

        if (innerClient != null) {
            if (logger.isInfoEnabled()) {
                logger.info("Inner client will shutdown");
            }
            innerClient.close();
        }

        for (APIListener apiListener : apiListeners) {
            if (apiListener instanceof AutoCloseable) {
                ((AutoCloseable) apiListener).close();
            }
            ZookeeperClient.closeClient();
        }
    }

    @Override
    public TopicCoordinator getTopicCoordinator() {
        return topicCoordinator;
    }

    @Override
    public ClusterCoordinator getClusterCoordinator() {
        return clusterCoordinator;
    }

    @Override
    public LogCoordinator getLogCoordinator() {
        return logCoordinator;
    }

    @Override
    public ConnectionCoordinator getConnectionCoordinator() {
        return connectionCoordinator;
    }

    @Override
    public void addMetricsListener(MetricsListener listener) {
        if (logCoordinator != null) {
            logCoordinator.addLogListener(Collections.singletonList(listener));
        }

        if (topicCoordinator != null) {
            topicCoordinator.addTopicListener(listener);
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
    public Client getInnerClient() {
        return innerClient;
    }
}
