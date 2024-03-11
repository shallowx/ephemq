package org.meteor.proxy.coordinatior;

import io.netty.util.concurrent.EventExecutor;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.coordinatior.DefaultConnectionCoordinator;
import org.meteor.coordinatior.DefaultCoordinator;
import org.meteor.ledger.LogCoordinator;
import org.meteor.listener.DefaultClusterListener;
import org.meteor.proxy.internal.ProxyServerConfig;
import org.meteor.remote.util.NetworkUtil;

public class ProxyDefaultCoordinator extends DefaultCoordinator implements ProxyCoordinator {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ProxyCoordinator.class);
    private final LedgerSyncCoordinator syncCoordinator;
    private volatile boolean state = false;

    public ProxyDefaultCoordinator(ProxyServerConfig configuration) {
        super(configuration);
        this.connectionCoordinator = new DefaultConnectionCoordinator();
        this.handleGroup = NetworkUtil.newEventExecutorGroup(configuration.getCommonConfig().getCommandHandleThreadLimit(), "proxy-handle");
        this.storageGroup = NetworkUtil.newEventExecutorGroup(configuration.getMessageConfig().getMessageStorageThreadLimit(), "proxy-storage");
        this.dispatchGroup = NetworkUtil.newEventExecutorGroup(configuration.getMessageConfig().getMessageDispatchThreadLimit(), "proxy-dispatch");
        this.syncGroup = NetworkUtil.newEventExecutorGroup(configuration.getMessageConfig().getMessageSyncThreadLimit(), "proxy-sync");
        this.auxGroup = NetworkUtil.newEventExecutorGroup(configuration.getCommonConfig().getAuxThreadLimit(), "proxy-aux");
        for (EventExecutor executor : auxGroup) {
            this.auxEventExecutors.add(executor);
        }

        this.syncCoordinator = new ProxyLedgerSyncCoordinator(configuration.getProxyConfiguration(), this);
        this.topicCoordinator = new ZookeeperProxyTopicCoordinator(configuration.getProxyConfiguration(), this);
        this.clusterCoordinator = new ZookeeperProxyClusterCoordinator(configuration);
        this.clusterCoordinator.addClusterListener(new DefaultClusterListener(this, configuration.getNetworkConfig()));
        this.logCoordinator = new LogCoordinator(configuration, this);
    }

    @Override
    public void start() throws Exception {
        if (!isRunning()) {
            super.start();
            this.syncCoordinator.start();
        } else {
            // keep empty
        }
    }

    private boolean isRunning() {
        return state;
    }

    @Override
    public LedgerSyncCoordinator getLedgerSyncCoordinator() {
        return syncCoordinator;
    }

    @Override
    public void shutdown() throws Exception {
        if (isRunning()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Proxy default coordinator is closing");
            }
            super.shutdown();
            this.syncCoordinator.shutDown();
        } else {
            // keep empty
        }
    }
}
