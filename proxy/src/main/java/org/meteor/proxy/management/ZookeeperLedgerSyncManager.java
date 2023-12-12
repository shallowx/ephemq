package org.meteor.proxy.management;

import com.google.inject.Inject;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Promise;
import org.apache.curator.framework.CuratorFramework;
import org.meteor.core.CoreConfig;
import org.meteor.client.internal.ClientChannel;
import org.meteor.common.Node;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.ledger.Log;
import org.meteor.management.JsonMapper;
import org.meteor.management.Manager;
import org.meteor.management.ZookeeperClient;
import org.meteor.proxy.Proxy;
import org.meteor.proxy.ProxyLog;

import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.TimeUnit;

public class ZookeeperLedgerSyncManager extends LedgerSyncManager{
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(Proxy.class);

    private final WeakHashMap<ProxyLog, Long> dispatchTotal = new WeakHashMap<>();
    private long commitTime = System.currentTimeMillis();

    @Inject
    public ZookeeperLedgerSyncManager(CoreConfig config, Manager manager) {
        super(config, manager);
        EventExecutor executor = manager.getAuxEventExecutorGroup().next();
        executor.scheduleAtFixedRate(() -> {
            try {
                commitLoad();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }, config.getProxySyncCheckIntervalMs(), config.getProxySyncCheckIntervalMs(), TimeUnit.MILLISECONDS);
    }

    private void commitLoad() throws Exception {
        long now = System.currentTimeMillis();
        for (Log log : manager.getLogManager().getLedgerId2LogMap().values()) {
            ProxyLog proxyLog = (ProxyLog) log;
            ClientChannel syncChannel = log.getSyncChannel();
            if (syncChannel == null) {
                continue;
            }
            if ((now - proxyLog.getLastSubscribeTimeMs()) > 60 * 1000 && log.getSubscriberCount() == 0) {
                int ledger = log.getLedger();
                Promise<Boolean> promise = deSyncAndCloseIfNotSubscribe(proxyLog);
                try {
                    promise.get();
                } catch (Exception e) {
                    logger.error("destroy log error error, ledger={} topic={}", ledger, log.getTopic(), e);
                }
            }
        }

        CuratorFramework client = ZookeeperClient.getClient(config, config.getClusterName());
        String path = String.format(ZookeeperPathConstants.PROXIES_ID, config.getServerId());
        byte[] bytes = client.getData().forPath(path);
        Node proxyNode = JsonMapper.deserialize(bytes, Node.class);
        proxyNode.setLedgerThroughput(calculateLedgerThroughput());
        client.setData().forPath(path, JsonMapper.serialize(proxyNode));
    }

    private Map<Integer, Integer> calculateLedgerThroughput() {
        Map<Integer, Integer> ret = new HashMap<>();
        long interval = (System.currentTimeMillis() - commitTime) / 1000;
        for (Log log : manager.getLogManager().getLedgerId2LogMap().values()) {
            ProxyLog proxyLog = (ProxyLog) log;
            long newValue = proxyLog.getDispatchTotal();
            Long oldValue = dispatchTotal.put(proxyLog, newValue);
            if (oldValue == null || oldValue <= 0) {
                continue;
            }
            int throughput = (int) ((newValue - oldValue) / interval);
            if (throughput > 0 ) {
                ret.put(log.getLedger(), throughput);
            }
        }
        commitTime = System.currentTimeMillis();
        return ret;
    }
}
