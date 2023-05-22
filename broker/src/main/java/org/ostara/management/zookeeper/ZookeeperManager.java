package org.ostara.management.zookeeper;

import com.google.inject.Inject;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import org.ostara.client.ClientConfig;
import org.ostara.client.internal.Client;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.core.Config;
import org.ostara.core.InnerClientListener;
import org.ostara.core.inner.InnerClient;
import org.ostara.listener.*;
import org.ostara.log.LogManager;
import org.ostara.management.*;
import org.ostara.remote.util.NetworkUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class ZookeeperManager implements Manager {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ZookeeperManager.class);

    private LogManager logManager;
    private TopicManager topicManager;
    private ClusterManager clusterManager;
    private Config config;
    private ConnectionManager connectionManager;
    private List<APIListener> apiListeners = new LinkedList<>();
    private EventExecutorGroup handleGroup;
    private EventExecutorGroup storageGroup;
    private EventExecutorGroup dispatchGroup;
    private EventExecutorGroup auxGroup;
    private List<EventExecutor> auxEventExecutors;
    private Client innerClient;

    public ZookeeperManager() {
    }

    @Inject
    public ZookeeperManager(Config config) {
        this.config = config;
        this.connectionManager = new DefaultConnectionManager();
        this.handleGroup = NetworkUtils.newEventExecutorGroup(config.getCommandHandleThreadCounts(), "command-handle-group");
        this.storageGroup = NetworkUtils.newEventExecutorGroup(config.getMessageStorageThreadCounts(), "storage-group");
        this.dispatchGroup = NetworkUtils.newEventExecutorGroup(config.getMessageDispatchThreadCounts(), "dispatch-group");

        clusterManager = new ZookeeperClusterManager(config);
        ClusterListener clusterListener = new DefaultClusterListener(this, config);
        clusterManager.addClusterListener(clusterListener);

        logManager = new LogManager(config, this);
        topicManager = new ZookeeperTopicManager(config, this);
        TopicListener topicListener = new DefaultTopicListener(this, config);
        topicManager.addTopicListener(topicListener);

        auxGroup = NetworkUtils.newEventExecutorGroup(config.getAuxThreadCounts(), "aux-group");
        auxEventExecutors = new ArrayList<>(config.getAuxThreadCounts());
        for (EventExecutor executor : auxGroup) {
            auxEventExecutors.add(executor);
        }
    }

    @Override
    public void start() throws Exception {
        ClientConfig clientConfig = new ClientConfig();
        List<String> clusterNodeAddress = Collections.singletonList(config.getAdvertisedAddress() + ":" + config.getAdvertisedPort());

        clientConfig.setBootstrapAddresses(clusterNodeAddress);
        clientConfig.setChannelConnectionTimeoutMs(config.getConnectionTimeoutMs());
        clientConfig.setSocketEpollPrefer(true);
        clientConfig.setSocketReceiveBufferSize(524288);
        clientConfig.setSocketSendBufferSize(524288);
        innerClient = new InnerClient("inner-client", clientConfig, new InnerClientListener(config, this),  config, this);
        innerClient.start();

        if (clusterManager != null) {
            clusterManager.start();
        }

        if (topicManager != null) {
            topicManager.start();
        }

        if (logManager != null) {
            logManager.start();
        }
    }

    @Override
    public void shutdown() throws Exception {
        if (clusterManager != null) {
            clusterManager.shutdown();
        }
        if (topicManager != null) {
            topicManager.shutdown();
        }
        if (logManager != null) {
            logManager.shutdown();
        }

        if (handleGroup != null) {
            handleGroup.shutdownGracefully();
        }

        if (dispatchGroup != null) {
            dispatchGroup.shutdownGracefully();
        }

        if (storageGroup != null) {
            storageGroup.shutdownGracefully();
        }

        if (innerClient != null) {
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
    public TopicManager getTopicManager() {
        return topicManager;
    }

    @Override
    public ClusterManager getClusterManager() {
        return clusterManager;
    }

    @Override
    public LogManager getLogManager() {
        return logManager;
    }

    @Override
    public ConnectionManager getConnectionManager() {
        return connectionManager;
    }

    @Override
    public void addMetricsListener(MetricsListener listener) {
        if (logManager != null) {
            logManager.addlogListener(Collections.singletonList(listener));
        }

        if (topicManager != null) {
            topicManager.addTopicListener(listener);
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
