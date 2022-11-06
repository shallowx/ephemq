package org.shallow.internal.metadata;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.netty.util.concurrent.*;
import io.netty.util.concurrent.ScheduledFuture;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.shallow.client.Client;
import org.shallow.client.internal.ClientChannel;
import org.shallow.client.internal.OperationInvoker;
import org.shallow.client.pool.ShallowChannelPool;
import org.shallow.common.logging.InternalLogger;
import org.shallow.common.logging.InternalLoggerFactory;
import org.shallow.common.meta.NodeRecord;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.proto.NodeMetadata;
import org.shallow.proto.heartbeat.HeartbeatRequest;
import org.shallow.proto.heartbeat.HeartbeatResponse;
import org.shallow.proto.server.*;
import org.shallow.remote.util.NetworkUtil;

import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static org.shallow.remote.processor.ProcessCommand.Nameserver.HEARTBEAT;
import static org.shallow.remote.processor.ProcessCommand.Nameserver.QUERY_NODE;
import static org.shallow.remote.processor.ProcessCommand.Server.REGISTER_NODE;
import static org.shallow.remote.processor.ProcessCommand.Server.UN_REGISTER_NODE;
import static org.shallow.remote.util.NetworkUtil.newEventExecutorGroup;
import static org.shallow.remote.util.NetworkUtil.switchSocketAddress;

public class ClusterNodeCache {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ClusterNodeCache.class);

    private final BrokerConfig config;
    private final Client internalClient;
    private final LoadingCache<String, Set<NodeRecord>> cache;
    private final ScheduledExecutorService scheduledExecutor;
    private final CountDownLatch latch = new CountDownLatch(1);

    public ClusterNodeCache(BrokerConfig config, Client internalClient) {
        this.config = config;
        this.internalClient = internalClient;
        this.cache = Caffeine.newBuilder().refreshAfterWrite(1, TimeUnit.MINUTES)
                .build(new CacheLoader<>() {
                    @Override
                    public @Nullable Set<NodeRecord> load(String key) throws Exception {
                        try {
                            return loadFromNameserver(key);
                        } catch (Exception e) {
                            return null;
                        }
                    }
                });

        this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("heartbeat"));
    }

    public void start() throws Exception {
        ScheduledFuture<?> registerStartFuture = newEventExecutorGroup(1, "socket-start")
                .next()
                .schedule(() -> {
                    try {
                        Promise<Void> promise = NetworkUtil.newImmediatePromise();
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
        scheduledExecutor.scheduleWithFixedDelay(this::heartbeat, 0, config.getHeartbeatScheduleFixedDelayMs(), TimeUnit.MILLISECONDS);
    }

    public void register(Promise<Void> promise) throws Exception {
        OperationInvoker invoker = acquireInvokerByRandomClientChannel();
        NodeRegistrationRequest request = NodeRegistrationRequest.newBuilder()
                .setCluster(config.getClusterName())
                .setServer(config.getServerId())
                .setHost(config.getExposedHost())
                .setPort(config.getExposedPort())
                .build();
        invoker.invoke(REGISTER_NODE, config.getInvokeTimeMs(), promise, request, NodeRegistrationResponse.class);
    }

    public void unregister() throws Exception {
        NodeUnregistrationRequest request = NodeUnregistrationRequest.newBuilder()
                .setCluster(config.getClusterName())
                .setServer(config.getServerId())
                .build();

        Promise<Void> promise = NetworkUtil.newImmediatePromise();
        promise.addListener(future -> {});

        OperationInvoker invoker = acquireInvokerByRandomClientChannel();
        invoker.invoke(UN_REGISTER_NODE, config.getInvokeTimeMs(), promise, request, HeartbeatResponse.class);
    }

    public Set<NodeRecord> loadFromNameserver(String cluster) throws ExecutionException, InterruptedException {
        QueryClusterNodeRequest request = QueryClusterNodeRequest.newBuilder()
                .setCluster(cluster)
                .build();

        Promise<QueryClusterNodeResponse> promise = NetworkUtil.newImmediatePromise();

        OperationInvoker invoker = acquireInvokerByRandomClientChannel();
        invoker.invoke(QUERY_NODE, config.getInvokeTimeMs(), promise, request, QueryClusterNodeResponse.class);
        List<NodeMetadata> nodes = promise.get().getNodesList();

        if (nodes.isEmpty()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Query node record is empty");
            }
            return null;
        }

        return nodes.stream()
                .map(nodeMetadata -> NodeRecord
                        .newBuilder()
                        .cluster(nodeMetadata.getCluster())
                        .name(nodeMetadata.getName())
                        .state(nodeMetadata.getState())
                        .socketAddress(switchSocketAddress(nodeMetadata.getHost(), nodeMetadata.getPort()))
                        .build())
                .collect(Collectors.toSet());
    }

    private void heartbeat() {
        HeartbeatRequest request = HeartbeatRequest.newBuilder()
                .setCluster(config.getClusterName())
                .setServer(config.getServerId())
                .build();

        Promise<Void> promise = NetworkUtil.newImmediatePromise();

        OperationInvoker invoker = acquireInvokerByRandomClientChannel();
        invoker.invoke(HEARTBEAT, config.getInvokeTimeMs(), promise, request, HeartbeatResponse.class);
    }

    private OperationInvoker acquireInvokerByRandomClientChannel() {
        ShallowChannelPool chanelPool = internalClient.getChanelPool();
        ClientChannel clientChannel = chanelPool.acquireWithRandomly();
        return clientChannel.invoker();
    }

    public void shutdown() throws Exception {
        scheduledExecutor.shutdown();
        unregister();
    }
}
