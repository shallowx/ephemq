package org.leopard.internal.metadata;

import static org.leopard.remote.util.NetworkUtils.newEventExecutorGroup;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Promise;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.leopard.common.logging.InternalLogger;
import org.leopard.common.logging.InternalLoggerFactory;
import org.leopard.common.metadata.Node;
import org.leopard.common.util.StringUtils;
import org.leopard.internal.config.ServerConfig;
import org.leopard.remote.util.NetworkUtils;

public class ClusterNodeCacheWriterSupport {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ClusterNodeCacheWriterSupport.class);

    private final ServerConfig config;
    private final LoadingCache<String, Set<Node>> cache;
    private final ScheduledExecutorService heartbeatScheduledExecutor;
    private final ScheduledExecutorService registryScheduledExecutor;
    private final ScheduledExecutorService cleanClosedNodeScheduledExecutor;

    private final CountDownLatch latch = new CountDownLatch(1);

    private final String STARTED = "started";
    private final String CLOSED = "closed";
    private final String LATENT = "latent";

    public ClusterNodeCacheWriterSupport(ServerConfig config) {
        this.config = config;
        this.cache = Caffeine.newBuilder().refreshAfterWrite(config.getMetadataRefreshMs(), TimeUnit.MILLISECONDS)
                .build(new CacheLoader<>() {
                    @Override
                    public @Nullable Set<Node> load(String key) throws Exception {
                        Set<Node> nodes = new HashSet<>();
                        nodes.add(buildNode(key, STARTED));
                        return nodes;
                    }
                });
        this.heartbeatScheduledExecutor =
                Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("heartbeat-scheduled-executor"));
        this.registryScheduledExecutor =
                Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("registry-scheduled-executor"));
        this.cleanClosedNodeScheduledExecutor =
                Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("clean-closed-scheduled-executor"));
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

        cleanClosedNodeScheduledExecutor.scheduleAtFixedRate(() -> {
            ConcurrentMap<String, Set<Node>> map = this.cache.asMap();
            if (map != null && !map.isEmpty()) {
                Set<Map.Entry<String, Set<Node>>> entries = map.entrySet();
                for (Map.Entry<String, Set<Node>> entry : entries) {
                    Set<Node> nodes = entry.getValue();
                    Iterator<Node> iterator = nodes.stream().iterator();
                    while (iterator.hasNext()) {
                        Node node = iterator.next();
                        if (node.getState().equals(CLOSED)) {
                            iterator.remove();
                        }
                    }
                }
            }
        }, 0, 30, TimeUnit.SECONDS);
    }

    public void register(Promise<Void> promise) throws Exception {
        String cluster = config.getClusterName();
        Set<Node> nodes = this.cache.get(cluster);
        if (nodes == null) {
            nodes = new HashSet<>();
        }
        nodes.add(buildNode(cluster, STARTED));
        this.cache.put(cluster, nodes);

        promise.trySuccess(null);
    }

    public void unregister() throws Exception {
        String cluster = config.getClusterName();
        Set<Node> nodes = this.cache.get(cluster);
        if (nodes != null && !nodes.isEmpty()) {
            Optional<Node> optional =
                    nodes.stream()
                            .filter(node -> node.getName().equals(config.getServerId()))
                            .findAny();

            if (optional.isPresent()) {
                Node node = optional.get();
                node.updateState(CLOSED);
            }
        }
    }

    private Node buildNode(String cluster, String state) {
        return Node.newBuilder()
                .cluster(cluster)
                .lastKeepLiveTime(System.currentTimeMillis())
                .name(config.getServerId())
                .state(state)
                .socketAddress(NetworkUtils.switchSocketAddress(config.getExposedHost(), config.getExposedPort()))
                .build();
    }

    public Set<Node> load(String cluster) throws Exception {
        if (StringUtils.isNullOrEmpty(cluster)) {
            cluster = config.getClusterName();
        }

        return this.cache.get(cluster).stream()
                .filter(node -> node.getState().equals(STARTED))
                .collect(Collectors.toSet());
    }

    public Node getThisNode() {
        return buildNode(config.getClusterName(), STARTED);
    }

    public int size() {
        return cache.asMap().size();
    }

    public void shutdown() throws Exception {
        heartbeatScheduledExecutor.shutdown();
        registryScheduledExecutor.shutdown();
        this.unregister();
    }
}
