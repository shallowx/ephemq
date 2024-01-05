package org.meteor.proxy.coordinatior;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Promise;
import org.apache.curator.framework.CuratorFramework;
import org.meteor.client.internal.ClientChannel;
import org.meteor.common.message.Node;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.proxy.internal.ProxyConfig;
import org.meteor.ledger.Log;
import org.meteor.coordinatior.JsonMapper;
import org.meteor.coordinatior.Coordinator;
import org.meteor.internal.ZookeeperClient;
import org.meteor.proxy.MeteorProxy;
import org.meteor.proxy.internal.ProxyLog;

import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.TimeUnit;

public class ProxyLedgerSyncCoordinator extends LedgerSyncCoordinator {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MeteorProxy.class);
    private final WeakHashMap<ProxyLog, Long> dispatchTotal = new WeakHashMap<>();
    private long commitTimeMillis = System.currentTimeMillis();
    private final ProxyConfig proxyConfiguration;

    public ProxyLedgerSyncCoordinator(ProxyConfig proxyConfiguration, Coordinator coordinator) {
        super(proxyConfiguration, coordinator);
        this.proxyConfiguration = proxyConfiguration;
        EventExecutor executor = coordinator.getAuxEventExecutorGroup().next();
        executor.scheduleAtFixedRate(() -> {
            try {
                commitLoad();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }, config.getProxyLeaderSyncInitialDelayMilliseconds(), config.getProxyLeaderSyncPeriodMilliseconds(), TimeUnit.MILLISECONDS);
    }

    private void commitLoad() throws Exception {
        long now = System.currentTimeMillis();
        for (Log log : coordinator.getLogCoordinator().getLedgerIdOfLogs().values()) {
            ProxyLog proxyLog = (ProxyLog) log;
            ClientChannel syncChannel = log.getSyncChannel();
            if (syncChannel == null) {
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

        CuratorFramework client = ZookeeperClient.getReadyClient(config.getZookeeperConfiguration(), proxyConfiguration.getCommonConfiguration().getClusterName());
        String path = String.format(ZookeeperPathConstants.PROXIES_ID, proxyConfiguration.getCommonConfiguration().getServerId());
        byte[] bytes = client.getData().forPath(path);
        Node proxyNode = JsonMapper.deserialize(bytes, Node.class);
        proxyNode.setLedgerThroughput(calculateLedgerThroughput());
        client.setData().forPath(path, JsonMapper.serialize(proxyNode));
    }

    private Map<Integer, Integer> calculateLedgerThroughput() {
        Map<Integer, Integer> ret = new HashMap<>();
        long interval = (System.currentTimeMillis() - commitTimeMillis) / 1000;
        for (Log log : coordinator.getLogCoordinator().getLedgerIdOfLogs().values()) {
            ProxyLog proxyLog = (ProxyLog) log;
            long newValue = proxyLog.getTotalDispatchedMessages();
            Long oldValue = dispatchTotal.put(proxyLog, newValue);
            if (oldValue == null || oldValue <= 0) {
                continue;
            }
            int throughput = (int) ((newValue - oldValue) / interval);
            if (throughput > 0 ) {
                ret.put(log.getLedger(), throughput);
            }
        }
        commitTimeMillis = System.currentTimeMillis();
        return ret;
    }
}
