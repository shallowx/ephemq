package org.meteor.proxy.support;

import io.netty.util.concurrent.EventExecutor;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.ledger.LogHandler;
import org.meteor.listener.DefaultClusterListener;
import org.meteor.proxy.core.ProxyServerConfig;
import org.meteor.remote.util.NetworkUtil;
import org.meteor.support.DefaultConnectionContainer;
import org.meteor.support.DefaultMeteorManager;

/**
 * ProxyDefaultManager is a class that extends DefaultMeteorManager
 * and implements the ProxyManager interface. It manages proxy
 * functionalities, handling network events, message storage,
 * dispatching, and synchronization.
 */
public class ProxyDefaultManager extends DefaultMeteorManager implements ProxyManager {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ProxyManager.class);
    /**
     * Provides support for synchronizing with the ledger.
     */
    private final LedgerSyncSupport syncSupport;
    /**
     * Represents the running state of the ProxyDefaultManager.
     * This variable is marked as volatile to ensure visibility
     * of changes to the state variable across multiple threads.
     */
    private volatile boolean state = false;

    /**
     * Constructs an instance of ProxyDefaultManager using the provided proxy server configuration.
     * This includes initializing various event executor groups, synchronization support,
     * topic handle support, cluster manager, and log handler pertaining to the proxy server.
     *
     * @param configuration the configuration object containing proxy server settings
     */
    public ProxyDefaultManager(ProxyServerConfig configuration) {
        super(configuration);
        this.connection = new DefaultConnectionContainer();
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

    /**
     * Starts the {@code ProxyDefaultManager}. This method ensures that the manager
     * and its supporting components are initialized and running. If the manager is
     * already running, it throws a {@code MeterProxyException}.
     *
     * @throws Exception           if an error occurs during the startup process
     * @throws MeterProxyException if the manager is already started
     */
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

    /**
     * Checks if the ProxyDefaultManager is currently running.
     *
     * @return true if the manager is running, false otherwise.
     */
    private boolean isRunning() {
        return state;
    }

    /**
     * Retrieves the ledger synchronization support instance from the {@code ProxyDefaultManager}.
     *
     * @return the {@code LedgerSyncSupport} instance used for synchronizing ledgers.
     */
    @Override
    public LedgerSyncSupport getLedgerSyncSupport() {
        return syncSupport;
    }

    /**
     * Shuts down the ProxyDefaultManager instance, ensuring that all associated resources and
     * components are properly released.
     * <p>
     * If the manager is currently running, it logs a debug message indicating that the shutdown
     * process is starting, calls the superclasses shutdown method to handle the shutdown of core
     * components, and then shuts down the ledger synchronization support.
     * <p>
     * If the manager is already closed, it logs a debug message stating that the manager is
     * already closed.
     *
     * @throws Exception if an error occurs during the shutdown process.
     */
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
