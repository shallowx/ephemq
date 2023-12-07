package org.ostara.proxy.management;

import com.google.inject.Inject;
import io.netty.util.concurrent.EventExecutor;
import org.ostara.beans.CoreConfig;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.ledger.LogManager;
import org.ostara.listener.DefaultClusterListener;
import org.ostara.management.DefaultConnectionManager;
import org.ostara.management.ZookeeperManager;
import org.ostara.remote.util.NetworkUtils;

public class ZookeeperProxyManager extends ZookeeperManager implements ProxyZookeeperManager {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ZookeeperProxyManager.class);
    private final LedgerSyncManager syncManager;

    @Inject
    public ZookeeperProxyManager(CoreConfig config) {
        super();
        this.config = config;
        this.connectionManager = new DefaultConnectionManager();
        this.handleGroup = NetworkUtils.newEventExecutorGroup(config.getCommandHandleThreadCounts(), "proxy-handle");
        this.storageGroup = NetworkUtils.newEventExecutorGroup(config.getMessageStorageThreadCounts(), "proxy-storage");
        this.dispatchGroup = NetworkUtils.newEventExecutorGroup(config.getMessageDispatchThreadCounts(), "proxy-dispatch");
        this.syncGroup = NetworkUtils.newEventExecutorGroup(config.getMessageSyncThreadCounts(), "proxy-sync");
        this.auxGroup = NetworkUtils.newEventExecutorGroup(config.getAuxThreadCounts(), "proxy-aux");
        for (EventExecutor executor : auxGroup) {
            this.auxEventExecutors.add(executor);
        }

        this.syncManager = new ZookeeperLedgerSyncManager(config, this);
        this.topicManager = new ZookeeperProxyTopicManager(config, this);
        this.clusterManager = new ZookeeperProxyClusterManager(config, syncManager);
        this.clusterManager.addClusterListener(new DefaultClusterListener(this, config));
        this.logManager = new LogManager(config, this);
    }

    @Override
    public LedgerSyncManager getLedgerSyncManager() {
        return syncManager;
    }

    @Override
    public void start() throws Exception {
        super.start();
        this.clusterManager.start();
    }

    @Override
    public void shutdown() throws Exception {
        super.shutdown();
        this.syncManager.shutDown();
    }
}
