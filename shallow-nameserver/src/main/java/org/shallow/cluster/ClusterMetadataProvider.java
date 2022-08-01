package org.shallow.cluster;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.protobuf.MessageLite;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.shallow.api.MappedFileAPI;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.meta.Node;
import org.shallow.proto.server.RegisterNodeResponse;
import org.shallow.util.JsonUtil;
import org.shallow.util.NetworkUtil;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.shallow.api.MappedFileConstants.TOPICS;
import static org.shallow.api.MappedFileConstants.Type.APPEND;
import static org.shallow.util.NetworkUtil.newImmediatePromise;

public class ClusterMetadataProvider {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ClusterMetadataProvider.class);

    private final EventExecutor cacheExecutor;
    private final EventExecutor apiExecutor;
    private final MappedFileAPI api;
    private final LoadingCache<String, List<Node>> clustersCache;

    public ClusterMetadataProvider(MappedFileAPI api, EventExecutor cacheExecutor, EventExecutor apiExecutor, long expired) {
        this.api = api;
        this.cacheExecutor = cacheExecutor;
        this.apiExecutor = apiExecutor;

        this.clustersCache = Caffeine.newBuilder()
                .expireAfterAccess(Long.MAX_VALUE, TimeUnit.DAYS)
                .expireAfterWrite(Long.MAX_VALUE, TimeUnit.DAYS)
                .build(new CacheLoader<>() {
                    @Override
                    public @Nullable List<Node> load(String key) throws Exception {
                        return null;
                    }
                });
    }

    public void write2CacheAndFile(String cluster, String name, String host, int port, Promise<MessageLite> promise) {
        if (cacheExecutor.inEventLoop()) {
            doWrite2CacheAndFile(cluster, name, host, port, promise);
        } else {
            cacheExecutor.execute(() -> {doWrite2CacheAndFile(cluster, name, host, port, promise);});
        }
    }

    private void doWrite2CacheAndFile(String cluster, String name, String host, int port, Promise<MessageLite> promise) {
        List<Node> clusterNodes = clustersCache.get(cluster) == null ? new CopyOnWriteArrayList<>() : clustersCache.get(cluster);

        clusterNodes.add(new Node(name, NetworkUtil.switchSocketAddress(host, port), System.currentTimeMillis()));
        final String nodes = JsonUtil.object2Json(getAllClusters());

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
                api.modify(TOPICS, nodes, APPEND, modifyPromise);
            } else {
                apiExecutor.execute(() -> api.modify(TOPICS, nodes, APPEND, modifyPromise));
            }
        } catch (Throwable t) {
        if (logger.isErrorEnabled()) {
            logger.error("[doWrite2CacheAndFile] - Failed to write node info to file, host={} port={}, cause:{}", host, port, t);
        }
        promise.tryFailure(t);
      }
    }

    public Map<String, List<Node>> getAllClusters() {
        return clustersCache.asMap();
    }
}
