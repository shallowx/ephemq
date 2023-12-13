package org.meteor.proxy.coordinatio;

import io.netty.util.concurrent.EventExecutor;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.configuration.ServerConfiguration;
import org.meteor.ledger.LogManager;
import org.meteor.listener.DefaultClusterListener;
import org.meteor.coordinatio.DefaultConnectionCoordinator;
import org.meteor.coordinatio.DefaultCoordinator;
import org.meteor.remote.util.NetworkUtils;

public class ProxyDefaultCoordinator extends DefaultCoordinator implements ProxyCoordinator {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ProxyCoordinator.class);
    private final LedgerSyncCoordinator syncManager;

    public ProxyDefaultCoordinator(ServerConfiguration configuration) {
        super(configuration);

        this.connectionManager = new DefaultConnectionCoordinator();
        this.handleGroup = NetworkUtils.newEventExecutorGroup(configuration.getCommonConfiguration().getCommandHandleThreadLimit(), "proxy-handle");
        this.storageGroup = NetworkUtils.newEventExecutorGroup(configuration.getMessageConfiguration().getMessageStorageThreadLimit(), "proxy-storage");
        this.dispatchGroup = NetworkUtils.newEventExecutorGroup(configuration.getMessageConfiguration().getMessageDispatchThreadLimit(), "proxy-dispatch");
        this.syncGroup = NetworkUtils.newEventExecutorGroup(configuration.getMessageConfiguration().getMessageSyncThreadLimit(), "proxy-sync");
        this.auxGroup = NetworkUtils.newEventExecutorGroup(configuration.getCommonConfiguration().getAuxThreadLimit(), "proxy-aux");
        for (EventExecutor executor : auxGroup) {
            this.auxEventExecutors.add(executor);
        }

        this.syncManager = new ProxyLedgerSyncCoordinator(configuration.getProxyConfiguration(), this);
        this.topicManager = new ZookeeperProxyTopicCoordinator(configuration.getProxyConfiguration(), this);
        this.clusterManager = new ZookeeperProxyClusterCoordinator(configuration);
        this.clusterManager.addClusterListener(new DefaultClusterListener(this, configuration.getNetworkConfiguration()));
        this.logManager = new LogManager(configuration, this);
    }

    @Override
    public LedgerSyncCoordinator getLedgerSyncManager() {
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
