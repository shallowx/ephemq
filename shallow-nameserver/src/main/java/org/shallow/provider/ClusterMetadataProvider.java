package org.shallow.provider;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.reflect.TypeToken;
import io.netty.util.concurrent.*;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.shallow.api.MappedFileAPI;
import org.shallow.internal.MetadataConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.proto.NodeMetadata;
import org.shallow.proto.server.HeartBeatResponse;
import org.shallow.proto.server.QueryClusterNodeResponse;
import org.shallow.proto.server.RegisterNodeResponse;
import org.shallow.util.JsonUtil;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.shallow.api.MappedFileConstants.CLUSTERS;
import static org.shallow.api.MappedFileConstants.TOPICS;
import static org.shallow.api.MappedFileConstants.Type.APPEND;
import static org.shallow.util.DateUtil.date2String;
import static org.shallow.util.DateUtil.date2TimeMillis;
import static org.shallow.util.JsonUtil.object2Json;
import static org.shallow.util.NetworkUtil.newImmediatePromise;
import static org.shallow.util.ObjectUtil.isNotNull;

public class ClusterMetadataProvider {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ClusterMetadataProvider.class);

    private final EventExecutor cacheExecutor;
    private final EventExecutor apiExecutor;
    private final MappedFileAPI api;
    private final MetadataConfig config;
    private final LoadingCache<String, Set<CacheNode>> activeNodes;
    private final LoadingCache<String, Set<CacheNode>> inactiveNodes;

    public ClusterMetadataProvider(MetadataConfig config, MappedFileAPI api, EventExecutorGroup group) {
        this.api = api;
        this.cacheExecutor = group.next();
        this.apiExecutor = group.next();
        this.config = config;

        this.activeNodes = Caffeine.newBuilder()
                .expireAfterAccess(Long.MAX_VALUE, TimeUnit.DAYS)
                .expireAfterWrite(Long.MAX_VALUE, TimeUnit.DAYS)
                .build(new CacheLoader<>() {
                    @Override
                    public @Nullable Set<CacheNode> load(String key) throws Exception {
                        return new CopyOnWriteArraySet<>();
                    }
                });

        this.inactiveNodes = Caffeine.newBuilder()
                .expireAfterAccess(Long.MAX_VALUE, TimeUnit.DAYS)
                .expireAfterWrite(Long.MAX_VALUE, TimeUnit.DAYS)
                .build(new CacheLoader<>() {
                    @Override
                    public @Nullable Set<CacheNode> load(String key) throws Exception {
                        return new CopyOnWriteArraySet<>();
                    }
                });

        cacheExecutor.scheduleWithFixedDelay(() -> scheduleWrite2File(), 60000, 60000, TimeUnit.MILLISECONDS);
    }

    public void start() throws Exception{
        this.populate();
        final Runnable activeTask = () -> checkNodeHeartAndHandleIfActiveOrNot(activeNodes, (now, cacheNode) -> {
            try {
                long lastHeartTime = date2TimeMillis(cacheNode.lastHeartTime);
                if ((now - lastHeartTime) > config.getHeartMaxIntervalTimeMs()) {
                    Set<CacheNode> exceptionCache = inactiveNodes.get(cacheNode.cluster);
                    exceptionCache.add(cacheNode);

                    if (logger.isWarnEnabled()) {
                        logger.warn("[activeTask] - the node<{}> was inactive and then transfer to inactive inspection task", cacheNode);
                    }
                    return true;
                }
            } catch (Exception ignored) {}
            return false;
        });
        cacheExecutor.scheduleWithFixedDelay(activeTask,0, config.getCheckHeartDelayTimeMs(), TimeUnit.MILLISECONDS);

        final Runnable inactiveTask = () -> checkNodeHeartAndHandleIfActiveOrNot(inactiveNodes, (now, cacheNode) -> {
            try {
                long lastHeartTime = date2TimeMillis(cacheNode.lastHeartTime);
                long interval = now - lastHeartTime;
                if (interval <= config.getHeartMaxIntervalTimeMs()) {
                    final Set<CacheNode> cacheNodes = activeNodes.get(cacheNode.cluster);
                    cacheNodes.add(cacheNode);

                    if (logger.isInfoEnabled()) {
                        logger.info("[inactiveTask] - the node<{}> was active and then transfer to active retry task", cacheNode);
                    }
                    return true;
                }

                if (interval > config.getHearCheckLastAvailableTimeMs()) {
                    return true;
                }
            } catch (Exception ignored) {}
            return false;
        });
        cacheExecutor.scheduleWithFixedDelay(inactiveTask,1000, config.getCheckHeartDelayTimeMs(), TimeUnit.MILLISECONDS);
    }

    public void keepHearBeat(String cluster, String name, String host, int port,Promise<HeartBeatResponse> promise) {
        if (cacheExecutor.inEventLoop()) {
            doKeepHeartBeat(cluster, name, host, port, promise);
        } else {
            cacheExecutor.execute(() -> doKeepHeartBeat(cluster, name, host, port, promise));
        }
    }

    private void doKeepHeartBeat(String cluster, String name, String host, int port,Promise<HeartBeatResponse> promise) {
       final Set<CacheNode> cacheNodes = activeNodes.get(cluster);
       final HeartBeatResponse.Builder builder = HeartBeatResponse.newBuilder()
               .setCluster(config.getServerId())
               .setHost(config.getExposedHost())
               .setPort(config.getExposedPort());

       final RuntimeException cause = new RuntimeException(String.format("The node<cluster=%s host=%s port=%s> failed to keep heartbeat with nameserver<cluster= %s host=%s port=%s>, " +
               "please check service state", cluster, host, port, config.getServerId(), config.getExposedHost(), config.getExposedPort()));

       if (cacheNodes.isEmpty()) {
           promise.tryFailure(cause);
           return;
       }

       final CacheNode cacheNode = cacheNodes.stream().filter(node -> node.name.equals(name)).findFirst().orElse(null);
       if (cacheNode == null) {
           promise.tryFailure(cause);
           return;
       }
       cacheNode.lastHeartTime = date2String(new Date());
       promise.trySuccess(builder.build());
    }

    public void queryActiveNodes(String cluster, Promise<QueryClusterNodeResponse> promise) {
        if (cacheExecutor.inEventLoop()) {
            doQueryActiveNodes(cluster, promise);
        } else {
            cacheExecutor.execute(() -> doQueryActiveNodes(cluster, promise));
        }
    }

    private void doQueryActiveNodes(String cluster, Promise<QueryClusterNodeResponse> promise) {
        final Set<CacheNode> nodes = activeNodes.get(cluster);
        QueryClusterNodeResponse.Builder builder = QueryClusterNodeResponse.newBuilder();
        if (nodes.isEmpty()) {
            promise.trySuccess(builder.build());
            return;
        }

        List<NodeMetadata> nodeMetadatas = nodes.stream()
                .map(p -> NodeMetadata.newBuilder()
                        .setName(p.name)
                        .setHost(p.host)
                        .setPort(p.port)
                        .build())
                .collect(Collectors.toList());

        promise.trySuccess(builder.addAllNodes(nodeMetadatas).build());
    }

    public void write2CacheAndFile(String cluster, String name, String host, int port, Promise<RegisterNodeResponse> promise) {
        if (cacheExecutor.inEventLoop()) {
            doWrite2CacheAndFile(cluster, name, host, port, promise);
        } else {
            cacheExecutor.execute(() -> {doWrite2CacheAndFile(cluster, name, host, port, promise);});
        }
    }

    private void doWrite2CacheAndFile(String cluster, String name, String host, int port, Promise<RegisterNodeResponse> promise) {
        final Set<CacheNode> clusterNodes = activeNodes.get(cluster);
        final Date now = new Date();
        final String time = date2String(now);
        clusterNodes.add(new CacheNode(cluster, name, host, port, time, time));
        activeNodes.put(cluster, clusterNodes);

        final String nodes = object2Json(getAllClusters());
        try {
            final Promise<Boolean> modifyPromise = newImmediatePromise();
            modifyPromise.addListener((GenericFutureListener<Future<Boolean>>) f -> {
                if (f.isSuccess()) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("[doWrite2CacheAndFile] - write node info to file successfully, content<{}>", nodes);
                    }
                    promise.trySuccess(RegisterNodeResponse.newBuilder().build());
                } else {
                    promise.tryFailure(f.cause());
                }
            });

            if (apiExecutor.inEventLoop()) {
                api.modify(CLUSTERS, nodes, APPEND, modifyPromise);
            } else {
                apiExecutor.execute(() -> api.modify(CLUSTERS, nodes, APPEND, modifyPromise));
            }
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("[doWrite2CacheAndFile] - failed to write node info to file, host={} port={}, cause:{}", host, port, t);
            }
        promise.tryFailure(t);
      }
    }

    public Map<String, Set<CacheNode>> getAllClusters() {
        return activeNodes.asMap();
    }

    public Set<CacheNode> getClusters(String cluster) {
        if (cluster == null || cluster.isEmpty()) {
            if (logger.isInfoEnabled()) {
                logger.info("[getClusters] - failed to get cluster node, because the cluster argument is empty");
            }
            return null;
        }

        final Set<CacheNode> cacheNodes = activeNodes.get(cluster);
        return (cacheNodes == null || cacheNodes.isEmpty()) ? null : cacheNodes;
    }

    private void checkNodeHeartAndHandleIfActiveOrNot(LoadingCache<String, Set<CacheNode>> cache, HeartFunction<Long, CacheNode, Boolean> function) {
        final Map<String, Set<CacheNode>> cacheNodes = cache.asMap();
        if (cacheNodes == null || cacheNodes.isEmpty()) {
            return;
        }

        final Set<Map.Entry<String, Set<CacheNode>>> entries = cacheNodes.entrySet();
        final Iterator<Map.Entry<String, Set<CacheNode>>> iterator = entries.iterator();
        final long now = System.currentTimeMillis();
        while (iterator.hasNext()) {
            Map.Entry<String, Set<CacheNode>> entry = iterator.next();
            Set<CacheNode> entryValue = entry.getValue();
            if (entryValue == null || entryValue.isEmpty()) {
                continue;
            }

            for (CacheNode cacheNode : entryValue) {
                if (function.accept(now, cacheNode)) {
                    entryValue.remove(cacheNode);
                    if (entryValue.isEmpty()) {
                        cache.invalidate(entry.getKey());
                    }
                }
            }
        }
    }

    private void populate() {
        final String partitions = api.read(CLUSTERS);
        final Map<String, Set<CacheNode>> clusters = JsonUtil.json2Object(partitions,
                new TypeToken<Map<String, List<CacheNode>>>() {}.getType());
        if (isNotNull(clusters) && !clusters.isEmpty()) {
            inactiveNodes.putAll(clusters);
        }
    }

    private void scheduleWrite2File() {
        final ConcurrentMap<String, Set<CacheNode>> topics = inactiveNodes.asMap();
        final String content = JsonUtil.object2Json(topics);
        api.modify(TOPICS, content, APPEND, newImmediatePromise());
    }

    @FunctionalInterface
    interface HeartFunction<T, E, R> {
        R accept(T t, E e);
    }

    public void shutdownGracefully() {

        cacheExecutor.shutdownGracefully();
        apiExecutor.shutdownGracefully();
    }

    private static class CacheNode {
        private String cluster;
        private String name;
        private String host;
        private int port;
        private String registerTime;
        private String lastHeartTime;

        public CacheNode(String cluster, String name, String host, int port, String registerTime, String lastHeartTime) {
            this.cluster = cluster;
            this.name = name;
            this.host = host;
            this.port = port;
            this.registerTime = registerTime;
            this.lastHeartTime = lastHeartTime;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof CacheNode cacheNode)) return false;
            return port == cacheNode.port &&
                    Objects.equals(cluster, cacheNode.cluster) &&
                    Objects.equals(name, cacheNode.name) &&
                    Objects.equals(host, cacheNode.host);
        }

        @Override
        public int hashCode() {
            return Objects.hash(cluster, name, host, port);
        }

        @Override
        public String toString() {
            return "CacheNode{" +
                    "cluster='" + cluster + '\'' +
                    ", name='" + name + '\'' +
                    ", host='" + host + '\'' +
                    ", port=" + port +
                    ", registerTime='" + registerTime + '\'' +
                    ", lastHeartTime='" + lastHeartTime + '\'' +
                    '}';
        }
    }
}
