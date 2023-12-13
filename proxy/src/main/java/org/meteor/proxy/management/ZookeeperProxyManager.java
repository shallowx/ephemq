package org.meteor.proxy.management;

import io.netty.util.concurrent.EventExecutor;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.configuration.ServerConfiguration;
import org.meteor.ledger.LogManager;
import org.meteor.listener.DefaultClusterListener;
import org.meteor.management.DefaultConnectionManager;
import org.meteor.management.ZookeeperManager;
import org.meteor.remote.util.NetworkUtils;

public class ZookeeperProxyManager extends ZookeeperManager implements ProxyZookeeperManager {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ZookeeperProxyManager.class);
    private final LedgerSyncManager syncManager;

    public ZookeeperProxyManager(ServerConfiguration configuration) {
        super(configuration);

        this.connectionManager = new DefaultConnectionManager();
        this.handleGroup = NetworkUtils.newEventExecutorGroup(configuration.getCommonConfiguration().getCommandHandleThreadLimit(), "proxy-handle");
        this.storageGroup = NetworkUtils.newEventExecutorGroup(configuration.getMessageConfiguration().getMessageStorageThreadLimit(), "proxy-storage");
        this.dispatchGroup = NetworkUtils.newEventExecutorGroup(configuration.getMessageConfiguration().getMessageDispatchThreadLimit(), "proxy-dispatch");
        this.syncGroup = NetworkUtils.newEventExecutorGroup(configuration.getMessageConfiguration().getMessageSyncThreadLimit(), "proxy-sync");
        this.auxGroup = NetworkUtils.newEventExecutorGroup(configuration.getCommonConfiguration().getAuxThreadLimit(), "proxy-aux");
        for (EventExecutor executor : auxGroup) {
            this.auxEventExecutors.add(executor);
        }

        this.syncManager = new ZookeeperLedgerSyncManager(configuration.getProxyConfiguration(), this);
        this.topicManager = new ZookeeperProxyTopicManager(configuration.getProxyConfiguration(), this);
        this.clusterManager = new ZookeeperProxyClusterManager(configuration);
        this.clusterManager.addClusterListener(new DefaultClusterListener(this, configuration.getNetworkConfiguration()));
        this.logManager = new LogManager(configuration, this);
    }

    @Override
    public LedgerSyncManager getLedgerSyncManager() {
        return syncManager;
    }

    @Override
    public void start() throws Exception {
        super.start();
        this.syncManager.start();
    }

    @Override
    public void shutdown() throws Exception {
        super.shutdown();
        this.syncManager.shutDown();
    }
}
