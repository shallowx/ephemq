package org.meteor.proxy.support;

import static org.meteor.support.SerializeFeatureSupport.deserialize;
import static org.meteor.support.SerializeFeatureSupport.serialize;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Promise;
import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.CuratorFramework;
import org.meteor.client.core.ClientChannel;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.common.message.Node;
import org.meteor.internal.ZookeeperClientFactory;
import org.meteor.ledger.Log;
import org.meteor.proxy.core.ProxyConfig;
import org.meteor.proxy.core.ProxyLog;
import org.meteor.support.Manager;

/**
 * ProxyLedgerSyncSupport is a final class that extends LedgerSyncSupport
 * and provides support for synchronizing the ledger's state with a proxy.
 * It schedules periodic tasks to commit load and synchronize data,
 * maintaining throughput counts and handling log de-subscription.
 */
final class ProxyLedgerSyncSupport extends LedgerSyncSupport {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ProxyLedgerSyncSupport.class);
    /**
     * A WeakHashMap that associates instances of ProxyLog with their corresponding dispatch totals in milliseconds.
     * The entries in this map are automatically removed when the key (ProxyLog) is no longer in use.
     */
    private final WeakHashMap<ProxyLog, Long> weakDispatchTotal = new WeakHashMap<>();
    /**
     * Holds the configuration settings required for setting up a proxy connection.
     * This is a final member variable, indicating that the configuration
     * cannot be modified after initial assignment.
     */
    private final ProxyConfig proxyConfiguration;
    /**
     * Represents the time in milliseconds when the commit was made.
     * This value is initialized to the current time when an instance of the containing class is created.
     */
    private long commitTimeMillis = System.currentTimeMillis();

    /**
     * Constructor for ProxyLedgerSyncSupport class.
     *
     * @param proxyConfiguration The configuration settings for the proxy.
     * @param manager            The manager that handles event execution.
     */
    public ProxyLedgerSyncSupport(ProxyConfig proxyConfiguration, Manager manager) {
        super(proxyConfiguration, manager);
        this.proxyConfiguration = proxyConfiguration;
        EventExecutor executor = manager.getAuxEventExecutorGroup().next();
        executor.scheduleAtFixedRate(() -> {
            try {
                commitLoad();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }, proxyConfig.getProxyLeaderSyncInitialDelayMilliseconds(), proxyConfig.getProxyLeaderSyncPeriodMilliseconds(), TimeUnit.MILLISECONDS);
    }

    /**
     * Commits the current load by iterating through all logs managed by the log handler,
     * checking each log's sync channel and subscriber status, and desynchronizing and
     * closing logs if they are not subscribed to and have exceeded a specific time threshold.
     * Also updates the ledger throughput information in the ZooKeeper node.
     *
     * @throws Exception if there's any error while attempting to process the logs
     *                   or interact with ZooKeeper.
     */
    private void commitLoad() throws Exception {
        long now = System.currentTimeMillis();
        for (Log log : manager.getLogHandler().getLedgerIdOfLogs().values()) {
            ProxyLog proxyLog = (ProxyLog) log;
            ClientChannel syncChannel = log.getSyncChannel();
            if (syncChannel == null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Sync channel is NULL");
                }
                continue;
            }
            if ((now - proxyLog.getLastSubscribeTimeMillis()) > 60 * 1000 && log.getSubscriberCount() == 0) {
                int ledger = log.getLedger();
                Promise<Boolean> promise = deSyncAndCloseIfNotSubscribe(proxyLog);
                try {
                    promise.get();
                } catch (Exception e) {
                    if (logger.isErrorEnabled()) {
                        logger.error("Destroy log error error, ledger[{}] topic[{}]", ledger, log.getTopic(), e);
                    }
                }
            }
        }

        CuratorFramework client = ZookeeperClientFactory.getReadyClient(proxyConfig.getZookeeperConfiguration(), proxyConfiguration.getCommonConfiguration().getClusterName());
        String path = ZookeeperProxyPathConstants.PROXIES_ID.formatted(proxyConfiguration.getCommonConfiguration().getServerId());
        byte[] bytes = client.getData().forPath(path);
        Node proxyNode = deserialize(bytes, Node.class);
        proxyNode.setLedgerThroughput(calculateLedgerThroughput());
        client.setData().forPath(path, serialize(proxyNode));
    }

    /**
     * Calculates the throughput for each ledger based on dispatched messages.
     * The throughput is determined by the difference in dispatched message count
     * over a time interval since the last calculation.
     *
     * @return A map where the keys are ledger IDs and the values are the
     *         calculated throughputs for those ledgers.
     */
    private Map<Integer, Integer> calculateLedgerThroughput() {
        Map<Integer, Integer> throughputCounts = new HashMap<>();
        long interval = (System.currentTimeMillis() - commitTimeMillis) / 1000;
        for (Log log : manager.getLogHandler().getLedgerIdOfLogs().values()) {
            ProxyLog proxyLog = (ProxyLog) log;
            long newValue = proxyLog.getTotalDispatchedMessages();
            Long oldValue = weakDispatchTotal.put(proxyLog, newValue);
            if (oldValue == null || oldValue <= 0) {
                continue;
            }
            int throughput = (int) ((newValue - oldValue) / interval);
            if (throughput > 0) {
                throughputCounts.put(log.getLedger(), throughput);
            }
        }
        commitTimeMillis = System.currentTimeMillis();
        return throughputCounts;
    }
}
