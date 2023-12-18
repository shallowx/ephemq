package org.meteor.proxy.coordinatior;

import io.netty.util.concurrent.EventExecutor;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.ledger.LogCoordinator;
import org.meteor.listener.DefaultClusterListener;
import org.meteor.coordinatior.DefaultConnectionCoordinator;
import org.meteor.coordinatior.DefaultCoordinator;
import org.meteor.proxy.internal.ProxyServerConfig;
import org.meteor.remote.util.NetworkUtils;

public class ProxyDefaultCoordinator extends DefaultCoordinator implements ProxyCoordinator {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ProxyCoordinator.class);
    private final LedgerSyncCoordinator syncCoordinator;

    public ProxyDefaultCoordinator(ProxyServerConfig configuration) {
        super(configuration);

        this.connectionCoordinator = new DefaultConnectionCoordinator();
        this.handleGroup = NetworkUtils.newEventExecutorGroup(configuration.getCommonConfiguration().getCommandHandleThreadLimit(), "proxy-handle");
        this.storageGroup = NetworkUtils.newEventExecutorGroup(configuration.getMessageConfiguration().getMessageStorageThreadLimit(), "proxy-storage");
        this.dispatchGroup = NetworkUtils.newEventExecutorGroup(configuration.getMessageConfiguration().getMessageDispatchThreadLimit(), "proxy-dispatch");
        this.syncGroup = NetworkUtils.newEventExecutorGroup(configuration.getMessageConfiguration().getMessageSyncThreadLimit(), "proxy-sync");
        this.auxGroup = NetworkUtils.newEventExecutorGroup(configuration.getCommonConfiguration().getAuxThreadLimit(), "proxy-aux");
        for (EventExecutor executor : auxGroup) {
            this.auxEventExecutors.add(executor);
        }

        this.syncCoordinator = new ProxyLedgerSyncCoordinator(configuration.getProxyConfiguration(), this);
        this.topicCoordinator = new ZookeeperProxyTopicCoordinator(configuration.getProxyConfiguration(), this);
        this.clusterCoordinator = new ZookeeperProxyClusterCoordinator(configuration);
        this.clusterCoordinator.addClusterListener(new DefaultClusterListener(this, configuration.getNetworkConfiguration()));
        this.logCoordinator = new LogCoordinator(configuration, this);
    }

    @Override
    public LedgerSyncCoordinator getLedgerSyncCoordinator() {
        return syncCoordinator;
    }

    @Override
    public void start() throws Exception {
        super.start();
        this.syncCoordinator.start();
    }

    @Override
    public void shutdown() throws Exception {
        super.shutdown();
        this.syncCoordinator.shutDown();
    }
}
