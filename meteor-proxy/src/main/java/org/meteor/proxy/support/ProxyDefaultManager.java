package org.meteor.proxy.support;

import io.netty.util.concurrent.EventExecutor;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.ledger.LogHandler;
import org.meteor.listener.DefaultClusterListener;
import org.meteor.proxy.core.ProxyServerConfig;
import org.meteor.remote.util.NetworkUtil;
import org.meteor.support.DefaultConnectionArraySet;
import org.meteor.support.DefaultMeteorManager;

public class ProxyDefaultManager extends DefaultMeteorManager implements ProxyManager {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ProxyManager.class);
    private final LedgerSyncSupport syncSupport;
    private volatile boolean state = false;

    public ProxyDefaultManager(ProxyServerConfig configuration) {
        super(configuration);
        this.connection = new DefaultConnectionArraySet();
        this.handleGroup = NetworkUtil.newEventExecutorGroup(configuration.getCommonConfig().getCommandHandleThreadLimit(), "proxy-handle");
        this.storageGroup = NetworkUtil.newEventExecutorGroup(configuration.getMessageConfig().getMessageStorageThreadLimit(), "proxy-storage");
        this.dispatchGroup = NetworkUtil.newEventExecutorGroup(configuration.getMessageConfig().getMessageDispatchThreadLimit(), "proxy-dispatch");
        this.syncGroup = NetworkUtil.newEventExecutorGroup(configuration.getMessageConfig().getMessageSyncThreadLimit(), "proxy-sync");
        this.auxGroup = NetworkUtil.newEventExecutorGroup(configuration.getCommonConfig().getAuxThreadLimit(), "proxy-aux");
        for (EventExecutor executor : auxGroup) {
            this.auxEventExecutors.add(executor);
        }

        this.syncSupport = new ProxyLedgerSyncSupport(configuration.getProxyConfiguration(), this);
        this.support = new ZookeeperProxyTopicHandleSupport(configuration.getProxyConfiguration(), this);
        this.clusterManager = new ZookeeperProxyClusterManager(configuration);
        this.clusterManager.addClusterListener(new DefaultClusterListener(this, configuration.getNetworkConfig()));
        this.logHandler = new LogHandler(configuration, this);
    }

    @Override
    public void start() throws Exception {
        if (!isRunning()) {
            super.start();
            this.syncSupport.start();
            this.state = true;
        } else {
            throw new MeterProxyException("ProxyDefaultManager is already started");
        }
    }

    private boolean isRunning() {
        return state;
    }

    @Override
    public LedgerSyncSupport getLedgerSyncSupport() {
        return syncSupport;
    }

    @Override
    public void shutdown() throws Exception {
        if (isRunning()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Proxy default support is closing");
            }
            super.shutdown();
            this.syncSupport.shutDown();
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("Proxy default support is closed");
            }
        }
    }
}
