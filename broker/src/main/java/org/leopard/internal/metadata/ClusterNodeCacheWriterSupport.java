package org.leopard.internal.metadata;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Promise;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.leopard.common.logging.InternalLogger;
import org.leopard.common.logging.InternalLoggerFactory;
import org.leopard.common.metadata.Node;
import org.leopard.common.util.StringUtils;
import org.leopard.internal.config.ServerConfig;
import org.leopard.remote.util.NetworkUtils;

import java.util.Set;
import java.util.concurrent.*;

import static org.leopard.remote.util.NetworkUtils.newEventExecutorGroup;

public class ClusterNodeCacheWriterSupport {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ClusterNodeCacheWriterSupport.class);

    private final ServerConfig config;
    private final LoadingCache<String, Set<Node>> cache;
    private final ScheduledExecutorService heartbeatScheduledExecutor;
    private final ScheduledExecutorService registryScheduledExecutor;
    private final CountDownLatch latch = new CountDownLatch(1);

    public ClusterNodeCacheWriterSupport(ServerConfig config) {
        this.config = config;
        this.cache = Caffeine.newBuilder().refreshAfterWrite(1, TimeUnit.MINUTES)
                .build(new CacheLoader<>() {
                    @Override
                    public @Nullable Set<Node> load(String key) throws Exception {
                        return null;
                    }
                });

        this.heartbeatScheduledExecutor = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("heartbeat"));
        this.registryScheduledExecutor = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("registry"));
    }

    public void start() throws Exception {
        ScheduledFuture<?> registerStartFuture = newEventExecutorGroup(1, "socket-start")
                .next()
                .schedule(() -> {
                    try {
                        Promise<Void> promise = NetworkUtils.newImmediatePromise();
                        promise.addListener(future -> {
                            if (future.isSuccess()) {
                                latch.countDown();
                            }
                        });

                        register(promise);
                    } catch (Exception e) {
                        if (logger.isErrorEnabled()) {
                            logger.error("Failed to register cluster node", e);
                        }
                        latch.countDown();
                        throw new RuntimeException(e);
                    }
                }, 0, TimeUnit.MILLISECONDS);
        registerStartFuture.get();

        latch.await();
    }

    public void register(Promise<Void> promise) throws Exception {

    }

    public void unregister() throws Exception {

    }

    public Set<Node> load(String cluster) throws Exception {
        if (StringUtils.isNullOrEmpty(cluster)) {
            cluster = config.getClusterName();
        }
        return cache.get(cluster);
    }

    public int size() {
        return cache.asMap().size();
    }

    public void shutdown() throws Exception {
        heartbeatScheduledExecutor.shutdown();
        registryScheduledExecutor.shutdown();
        unregister();
    }
}
