package org.shallow.nameserver.metadata;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Promise;
import org.shallow.NameserverConfig;
import org.shallow.common.logging.InternalLogger;
import org.shallow.common.logging.InternalLoggerFactory;
import org.shallow.common.meta.NodeRecord;
import org.shallow.common.util.StringUtil;
import org.shallow.remote.util.NetworkUtil;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

public class ClusterManager {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ClusterManager.class);

    private final LoadingCache<String, Set<NodeRecord>> cache;
    private final NameserverConfig config;
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("clear"));

    public ClusterManager(NameserverConfig config) {
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
        },0, config.getHeartbeatDelayPeriod(), TimeUnit.MILLISECONDS);
    }

    public void register(String cluster, String name, String host, int port, Promise<Void> promise) {
        try {
            if (StringUtil.isNullOrEmpty(cluster)) {

                promise.tryFailure(new IllegalStateException("invalid parameter, cluster-name cannot be empty"));
            }

            if (StringUtil.isNullOrEmpty(name)) {

                promise.tryFailure(new IllegalStateException("invalid parameter, server-id cannot be empty"));
            }

            if (StringUtil.isNullOrEmpty(host)) {
                promise.tryFailure(new IllegalStateException("invalid parameter, server-host "));
            }

            if (port < 0 || port > 0xFFFF) {
                promise.tryFailure(new IllegalStateException(String.format("invalid parameter, and port=%d", port)));
            }

            NodeRecord record = NodeRecord.newBuilder()
                    .name(name)
                    .cluster(cluster)
                    .state("UP")
                    .socketAddress(NetworkUtil.switchSocketAddress(host, port))
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

            promise.trySuccess(null);
        } catch (Throwable t) {
            promise.tryFailure(t);
        }
    }

    public void unregister(String cluster, String server, Promise<Void> promise) {
        try {
            if (StringUtil.isNullOrEmpty(cluster)) {

                promise.tryFailure(new IllegalStateException("invalid parameter, cluster-name cannot be empty"));
            }

            if (StringUtil.isNullOrEmpty(server)) {

                promise.tryFailure(new IllegalStateException("invalid parameter, server-id cannot be empty"));
            }

            Set<NodeRecord> records = cache.get(cluster);
            records.removeIf(record -> record.getName().equals(server));
            promise.trySuccess(null);
        } catch (Throwable t) {
            promise.tryFailure(t);
        }
    }

    public Set<NodeRecord> load(String cluster) {
        if (StringUtil.isNullOrEmpty(cluster)) {
            return null;
        }
        return cache.get(cluster);
    }

    public void heartbeat(String cluster, String server) {
        try {
            if (StringUtil.isNullOrEmpty(cluster) || StringUtil.isNullOrEmpty(server)) {
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
