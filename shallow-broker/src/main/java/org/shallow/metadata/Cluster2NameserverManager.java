package org.shallow.metadata;

import io.netty.util.concurrent.*;
import org.shallow.ClientConfig;
import org.shallow.internal.BrokerConfig;
import org.shallow.internal.BrokerManager;
import org.shallow.invoke.ClientChannel;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.pool.DefaultChannelPoolFactory;
import org.shallow.pool.ShallowChannelPool;
import org.shallow.proto.NodeMetadata;
import org.shallow.proto.server.HeartBeatRequest;
import org.shallow.proto.server.HeartBeatResponse;
import org.shallow.proto.server.RegisterNodeRequest;
import org.shallow.proto.server.RegisterNodeResponse;
import org.shallow.util.NetworkUtil;

import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.shallow.processor.ProcessCommand.NameServer.HEART_BEAT;
import static org.shallow.processor.ProcessCommand.NameServer.REGISTER_NODE;
import static org.shallow.util.NetworkUtil.newImmediatePromise;
import static org.shallow.util.ObjectUtil.isNull;

public class Cluster2NameserverManager {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(Cluster2NameserverManager.class);

    private final BrokerManager manager;
    private final BrokerConfig config;
    private final ClientConfig clientConfig;
    private final ShallowChannelPool pool;
    private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("heart-single-pool"));

    public Cluster2NameserverManager(BrokerManager manager, ClientConfig clientConfig, BrokerConfig config) {
        this.manager = manager;
        this.config = config;
        this.clientConfig = clientConfig;
        this.pool = DefaultChannelPoolFactory.INSTANCE.acquireChannelPool();
    }

    public void start() throws Exception {
        final String[] address = config.getNameserverUrl().split(",");
        final List<SocketAddress> socketAddresses = NetworkUtil.switchSocketAddress(List.of(address));

        register2Nameserver(socketAddresses);
        scheduledExecutor.scheduleWithFixedDelay(() -> {
            doHeartBeat(socketAddresses);
        }, 5, config.getHeartSendIntervalTimeMs(), TimeUnit.MILLISECONDS);
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
                    throw new RuntimeException(String.format("The node<name=%s host=%s port=%s> failed to join the cluster<%s>", config.getServerId(), host, port, config.getClusterName()));
                }
            });
            write2Nameserver(socketAddresses, host, port, promise);
        }).start();

        try {
            promise.get(clientConfig.getDefaultInvokeExpiredMs(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new RuntimeException(String.format("The node<name=%s host=%s port=%s> failed to join the cluster<%s>", config.getServerId(), host, port, config.getClusterName()));
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

    private void doHeartBeat(List<SocketAddress> socketAddresses) {
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
            } else {
                if (logger.isDebugEnabled()) {
                    HeartBeatResponse response = future.get(clientConfig.getDefaultInvokeExpiredMs(), TimeUnit.MILLISECONDS);
                    logger.debug("[doHeartBeta] - keep heartbeat with nameserver<cluster={} host={} port={}> successfully", response.getCluster(), response.getHost(), response.getPort());
                }
            }
        });

        ClientChannel requestChannel = pool.acquireHealthyOrNew(address);
        requestChannel.invoker().invoke(HEART_BEAT, clientConfig.getDefaultInvokeExpiredMs(), promise, request, HeartBeatResponse.class);
    }
}
