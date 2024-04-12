package org.meteor.proxy.support;

import static org.meteor.support.JsonFeatureMapper.deserialize;
import static org.meteor.support.JsonFeatureMapper.serialize;
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
import org.meteor.proxy.MeteorProxy;
import org.meteor.proxy.core.ProxyConfig;
import org.meteor.proxy.core.ProxyLog;
import org.meteor.support.Manager;

final class ProxyLedgerSyncCoordinator extends LedgerSyncCoordinator {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MeteorProxy.class);
    private final WeakHashMap<ProxyLog, Long> weakDispatchTotal = new WeakHashMap<>();
    private final ProxyConfig proxyConfiguration;
    private long commitTimeMillis = System.currentTimeMillis();

    public ProxyLedgerSyncCoordinator(ProxyConfig proxyConfiguration, Manager coordinator) {
        super(proxyConfiguration, coordinator);
        this.proxyConfiguration = proxyConfiguration;
        EventExecutor executor = coordinator.getAuxEventExecutorGroup().next();
        executor.scheduleAtFixedRate(() -> {
            try {
                commitLoad();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }, proxyConfig.getProxyLeaderSyncInitialDelayMilliseconds(), proxyConfig.getProxyLeaderSyncPeriodMilliseconds(), TimeUnit.MILLISECONDS);
    }

    private void commitLoad() throws Exception {
        long now = System.currentTimeMillis();
        for (Log log : coordinator.getLogCoordinator().getLedgerIdOfLogs().values()) {
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
        String path = String.format(ZookeeperProxyPathConstants.PROXIES_ID, proxyConfiguration.getCommonConfiguration().getServerId());
        byte[] bytes = client.getData().forPath(path);
        Node proxyNode = deserialize(bytes, Node.class);
        proxyNode.setLedgerThroughput(calculateLedgerThroughput());
        client.setData().forPath(path, serialize(proxyNode));
    }

    private Map<Integer, Integer> calculateLedgerThroughput() {
        Map<Integer, Integer> throughputCounts = new HashMap<>();
        long interval = (System.currentTimeMillis() - commitTimeMillis) / 1000;
        for (Log log : coordinator.getLogCoordinator().getLedgerIdOfLogs().values()) {
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
