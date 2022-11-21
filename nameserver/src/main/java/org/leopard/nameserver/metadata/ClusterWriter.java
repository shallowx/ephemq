package org.leopard.nameserver.metadata;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Promise;
import org.leopard.NameserverConfig;
import org.leopard.common.logging.InternalLogger;
import org.leopard.common.logging.InternalLoggerFactory;
import org.leopard.common.metadata.NodeRecord;
import org.leopard.common.util.StringUtils;
import org.leopard.remote.util.NetworkUtils;

import javax.annotation.concurrent.ThreadSafe;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@ThreadSafe
public class ClusterWriter {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ClusterWriter.class);

    private final LoadingCache<String, Set<NodeRecord>> cache;
    private final NameserverConfig config;
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("clear"));

    public ClusterWriter(NameserverConfig config) {
        this.config = config;
        this.cache = Caffeine.newBuilder().build(key -> null);
    }

    public void start() throws Exception {
        executor.scheduleAtFixedRate(() -> {
            ConcurrentMap<String, Set<NodeRecord>> recordMap = cache.asMap();
            if (recordMap == null || recordMap.isEmpty()) {
                return;
            }

            Set<Map.Entry<String, Set<NodeRecord>>> entries = recordMap.entrySet();
            for (Map.Entry<String, Set<NodeRecord>> entry : entries) {
                Set<NodeRecord> records = entry.getValue();
                if (records == null || records.isEmpty()) {
                    continue;
                }

                Iterator<NodeRecord> iterator = records.iterator();
                while (iterator.hasNext()) {
                    NodeRecord record = iterator.next();
                    long now = System.currentTimeMillis();
                    if ((now - record.getLastKeepLiveTime()) > 120000) {
                        iterator.remove();
                        if (logger.isWarnEnabled()) {
                            logger.warn("Node heartbeat timeout, node={}", record.toString());
                        }
                    }
                }
            }
        }, 0, config.getHeartbeatDelayPeriod(), TimeUnit.MILLISECONDS);
    }

    public void register(String cluster, String name, String host, int port, Promise<Void> promise) {
        try {
            synchronized (cache) {
                if (StringUtils.isNullOrEmpty(cluster)) {

                    promise.tryFailure(new IllegalStateException("invalid parameter, cluster-name cannot be empty"));
                }

                if (StringUtils.isNullOrEmpty(name)) {

                    promise.tryFailure(new IllegalStateException("invalid parameter, server-id cannot be empty"));
                }

                if (StringUtils.isNullOrEmpty(host)) {
                    promise.tryFailure(new IllegalStateException("invalid parameter, server-host "));
                }

                if (port < 0 || port > 0xFFFF) {
                    promise.tryFailure(new IllegalStateException(String.format("invalid parameter, and port=%d", port)));
                }

                NodeRecord record = NodeRecord.newBuilder()
                        .name(name)
                        .cluster(cluster)
                        .state("UP")
                        .socketAddress(NetworkUtils.switchSocketAddress(host, port))
                        .lastKeepLiveTime(System.currentTimeMillis())
                        .build();

                Set<NodeRecord> records = cache.get(cluster);
                if (records == null) {
                    records = new HashSet<>();
                    records.add(record);
                    cache.put(cluster, records);
                } else {
                    records.add(record);
                }
            }
            promise.trySuccess(null);
        } catch (Throwable t) {
            promise.tryFailure(t);
        }
    }

    public void unregister(String cluster, String server, Promise<Void> promise) {
        try {
            synchronized (cache) {
                if (StringUtils.isNullOrEmpty(cluster)) {

                    promise.tryFailure(new IllegalStateException("invalid parameter, cluster-name cannot be empty"));
                }

                if (StringUtils.isNullOrEmpty(server)) {

                    promise.tryFailure(new IllegalStateException("invalid parameter, server-id cannot be empty"));
                }

                Set<NodeRecord> records = cache.get(cluster);
                records.removeIf(record -> record.getName().equals(server));
            }
            promise.trySuccess(null);
        } catch (Throwable t) {
            promise.tryFailure(t);
        }
    }

    public Set<NodeRecord> load(String cluster) {
        if (StringUtils.isNullOrEmpty(cluster)) {
            return null;
        }
        return cache.get(cluster);
    }

    public void heartbeat(String cluster, String server) {
        try {
            if (StringUtils.isNullOrEmpty(cluster) || StringUtils.isNullOrEmpty(server)) {
                return;
            }

            Set<NodeRecord> records = cache.get(cluster);
            for (NodeRecord record : records) {
                if (record.getName().equals(server)) {
                    record.updateLastKeepLiveTime(System.currentTimeMillis());
                }
            }
        } catch (Exception ignored) {
            // ignored
        }
    }
}
