package org.shallow.metadata;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.netty.util.concurrent.*;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.internal.BrokerManager;
import org.shallow.internal.config.Client2NameserverConfig;
import org.shallow.invoke.ClientChannel;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.meta.Node;
import org.shallow.pool.DefaultChannelPoolFactory;
import org.shallow.pool.ShallowChannelPool;
import org.shallow.proto.NodeMetadata;
import org.shallow.proto.server.*;

import java.net.SocketAddress;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.shallow.processor.ProcessCommand.NameServer.*;
import static org.shallow.util.NetworkUtil.newImmediatePromise;
import static org.shallow.util.NetworkUtil.switchSocketAddress;
import static org.shallow.util.ObjectUtil.isNull;

public class Cluster2NameserverManager {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(Cluster2NameserverManager.class);

    private final BrokerManager manager;
    private final BrokerConfig config;
    private final Client2NameserverConfig clientConfig;
    private final ShallowChannelPool pool;
    private final LoadingCache<String, Set<Node>> nodeCaches;

    public Cluster2NameserverManager(BrokerManager manager, Client2NameserverConfig clientConfig, BrokerConfig config) {
        this.manager = manager;
        this.config = config;
        this.clientConfig = clientConfig;
        this.pool = DefaultChannelPoolFactory.INSTANCE.acquireChannelPool();
        this.nodeCaches = Caffeine.newBuilder().build(new CacheLoader<>() {
            @Override
            public @Nullable Set<Node> load(String key) throws Exception {
                return queryFromNameserver(key);
            }
        });
    }

    public void start() throws Exception {
        final String[] address = config.getNameserverUrl().split(",");
        final List<SocketAddress> socketAddresses = switchSocketAddress(List.of(address));

        register2Nameserver(socketAddresses);

        // start populating the cluster node cache information from file</workDirectory/cluster.json>
        nodeCaches.get(config.getClusterName());

        new Heartbeat(socketAddresses);
    }

    public void register2Nameserver(List<SocketAddress> socketAddresses) throws Exception {
        final String host = config.getExposedHost();
        final int port = config.getExposedPort();

        if (isNull(socketAddresses) || socketAddresses.isEmpty()) {
            throw new IllegalArgumentException(String.format("[Register2Nameserver] - failed to register node<%s> to nameserver", host + ":" + port));
        }

        Promise<RegisterNodeResponse> promise = ImmediateEventExecutor.INSTANCE.newPromise();
        new Thread(() -> {
            promise.addListener((GenericFutureListener<Future<RegisterNodeResponse>>) f -> {
                if (f.isSuccess()) {
                    if (logger.isInfoEnabled()) {
                        logger.info("The node<name={} host={} port={}> join the cluster<{}> successfully", config.getServerId(), host, port, config.getClusterName());
                    }
                } else {
                    throw new RuntimeException(String.format("The node<name=%s host=%s port=%s> failed to join the cluster<%s>. cause: %s", config.getServerId(), host, port, config.getClusterName(), f.cause()));
                }
            });
            write2Nameserver(socketAddresses, host, port, promise);
        }).start();

        try {
            promise.get(clientConfig.getDefaultInvokeExpiredMs(), TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(String.format("The node<name=%s host=%s port=%s> failed to join the cluster<%s>. cause: %s", config.getServerId(), host, port, config.getClusterName(), e));
        }
    }

    private Promise<RegisterNodeResponse> write2Nameserver(List<SocketAddress> socketAddresses, String host, int port, Promise<RegisterNodeResponse> promise) {
        final SocketAddress address = socketAddresses.parallelStream().findAny().orElse(null);
        try {
            NodeMetadata nodeMetadata = NodeMetadata
                    .newBuilder()
                    .setName(config.getServerId())
                    .setHost(host)
                    .setPort(port)
                    .build();

            RegisterNodeRequest request = RegisterNodeRequest
                    .newBuilder()
                    .setCluster(config.getClusterName())
                    .setMetadata(nodeMetadata)
                    .build();

            ClientChannel requestChannel = pool.acquireHealthyOrNew(address);
            requestChannel.invoker().invoke(REGISTER_NODE, clientConfig.getDefaultInvokeExpiredMs(), promise, request, RegisterNodeResponse.class);
        }catch (Exception e){
            promise.tryFailure(e);
        }
        return promise;
    }

    private Set<Node> queryFromNameserver(String cluster) {
        final QueryClusterNodeRequest request = QueryClusterNodeRequest
                .newBuilder()
                .setCluster(cluster)
                .build();
        try {
            ClientChannel requestChannel = pool.acquireWithRandomly();

            final Promise<QueryClusterNodeResponse> promise = newImmediatePromise();
            requestChannel.invoker().invoke(QUERY_CLUSTER_IFO, clientConfig.getDefaultInvokeExpiredMs(), promise, request, QueryClusterNodeResponse.class);

            final QueryClusterNodeResponse response = promise.get(clientConfig.getDefaultInvokeExpiredMs(), TimeUnit.MILLISECONDS);
            return response.getNodesList()
                    .stream()
                    .map(p -> new Node(p.getName(), switchSocketAddress(p.getHost(), p.getPort())))
                    .collect(Collectors.toSet());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private class Heartbeat {
        private final List<SocketAddress> addresses;
        private final ScheduledExecutorService heartBeatTaskExecutor =
                Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("heart-single-pool"));

        public Heartbeat(List<SocketAddress> socketAddresses) {
            this.addresses = socketAddresses;

            heartBeatTaskExecutor.scheduleWithFixedDelay(() -> {
                registerHeartbeatScheduledTask(addresses);
            }, 5000, config.getHeartSendIntervalTimeMs(), TimeUnit.MILLISECONDS);
        }

        private void registerHeartbeatScheduledTask(List<SocketAddress> socketAddresses) {

            final SocketAddress address = socketAddresses.parallelStream().findAny().orElse(null);
            NodeMetadata nodeMetadata = NodeMetadata
                    .newBuilder()
                    .setName(config.getServerId())
                    .setHost(config.getExposedHost())
                    .setPort(config.getExposedPort())
                    .build();

            HeartBeatRequest request = HeartBeatRequest
                    .newBuilder()
                    .setCluster(config.getClusterName())
                    .setMetadata(nodeMetadata)
                    .build();

            Promise<HeartBeatResponse> promise = newImmediatePromise();
            promise.addListener((GenericFutureListener<Future<HeartBeatResponse>>) future -> {
                if (!future.isSuccess()) {
                    // alert to organization group
                    if (logger.isWarnEnabled()) {
                        logger.warn("[doHeartBeta] - failed to keep nameserver, trg again later. cause:{}", future.cause());
                    }
                } else {
                    if (logger.isDebugEnabled()) {
                        HeartBeatResponse response = future.get(clientConfig.getDefaultInvokeExpiredMs(), TimeUnit.MILLISECONDS);
                        logger.debug("[doHeartBeta] - keep heartbeat with nameserver<cluster={} host={} port={}> successfully", response.getCluster(), response.getHost(), response.getPort());
                    }
                }
            });

            ClientChannel requestChannel = pool.acquireHealthyOrNew(address);
            requestChannel.invoker().invoke(HEARTBEAT, clientConfig.getDefaultInvokeExpiredMs(), promise, request, HeartBeatResponse.class);
        }
    }
}
