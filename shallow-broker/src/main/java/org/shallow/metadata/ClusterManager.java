package org.shallow.metadata;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.netty.util.concurrent.*;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.internal.BrokerManager;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.meta.Node;
import org.shallow.pool.DefaultChannelPoolFactory;
import org.shallow.pool.ShallowChannelPool;
import org.shallow.proto.server.*;
import org.shallow.util.NetworkUtil;

import java.net.SocketAddress;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import static org.shallow.util.NetworkUtil.switchSocketAddress;
import static org.shallow.util.ObjectUtil.isNull;

public class ClusterManager {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ClusterManager.class);

    private final BrokerManager manager;
    private final BrokerConfig config;
    private final ShallowChannelPool pool;
    private final LoadingCache<String, Set<Node>> nodeCaches;

    public ClusterManager(BrokerManager manager, BrokerConfig config) {
        this.manager = manager;
        this.config = config;
        this.pool = DefaultChannelPoolFactory.INSTANCE.acquireChannelPool();
        this.nodeCaches = Caffeine.newBuilder().build(new CacheLoader<>() {
            @Override
            public @Nullable Set<Node> load(String key) throws Exception {
                return null;
            }
        });
    }

    public void start() throws Exception {
        final String[] address = config.getNameserverUrl().split(",");
        final List<SocketAddress> socketAddresses = switchSocketAddress(List.of(address));

        registerNode(socketAddresses);

        // start populating the cluster node cache information from file</workDirectory/cluster.json>
        nodeCaches.get(config.getClusterName());

        new Heartbeat(socketAddresses);
    }

    public void registerNode(List<SocketAddress> socketAddresses) throws Exception {
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
        }).start();

        try {
            promise.get(1000, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(String.format("The node<name=%s host=%s port=%s> failed to join the cluster<%s>. cause: %s", config.getServerId(), host, port, config.getClusterName(), e));
        }
    }

    private class Heartbeat {
        private final List<SocketAddress> addresses;

        private final EventExecutor heartBeatTaskExecutor =
                NetworkUtil.newEventExecutorGroup(1, "heart-single-pool").next();

        public Heartbeat(List<SocketAddress> socketAddresses) {
            this.addresses = socketAddresses;

            heartBeatTaskExecutor.scheduleWithFixedDelay(() -> {
                registerHeartbeatScheduledTask(addresses);
            }, 5000, config.getHeartSendIntervalTimeMs(), TimeUnit.MILLISECONDS);
        }

        private void registerHeartbeatScheduledTask(List<SocketAddress> socketAddresses) {

        }
    }
}
