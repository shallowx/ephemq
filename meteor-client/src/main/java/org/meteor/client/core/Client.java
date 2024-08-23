package org.meteor.client.core;

import static org.meteor.client.util.MessageConstants.CLIENT_NETTY_PENDING_TASK_NAME;
import static org.meteor.remote.util.NetworkUtil.newEventLoopGroup;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollDatagramChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.resolver.dns.DefaultDnsServerAddressStreamProvider;
import io.netty.resolver.dns.DnsNameResolverBuilder;
import io.netty.resolver.dns.RoundRobinDnsAddressResolverGroup;
import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import io.netty.util.concurrent.SucceededFuture;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.meteor.client.exception.MeteorClientException;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.common.message.TopicConfig;
import org.meteor.common.util.TopicPatternUtil;
import org.meteor.remote.proto.ClusterInfo;
import org.meteor.remote.proto.NodeMetadata;
import org.meteor.remote.proto.PartitionMetadata;
import org.meteor.remote.proto.TopicInfo;
import org.meteor.remote.proto.TopicMetadata;
import org.meteor.remote.proto.server.CalculatePartitionsRequest;
import org.meteor.remote.proto.server.CalculatePartitionsResponse;
import org.meteor.remote.proto.server.CreateTopicConfigRequest;
import org.meteor.remote.proto.server.CreateTopicRequest;
import org.meteor.remote.proto.server.CreateTopicResponse;
import org.meteor.remote.proto.server.DeleteTopicRequest;
import org.meteor.remote.proto.server.DeleteTopicResponse;
import org.meteor.remote.proto.server.MigrateLedgerRequest;
import org.meteor.remote.proto.server.MigrateLedgerResponse;
import org.meteor.remote.proto.server.QueryClusterInfoRequest;
import org.meteor.remote.proto.server.QueryClusterResponse;
import org.meteor.remote.proto.server.QueryTopicInfoRequest;
import org.meteor.remote.proto.server.QueryTopicInfoResponse;
import org.meteor.remote.util.NetworkUtil;

public class Client implements MeterBinder {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(Client.class);
    private final Map<SocketAddress, List<Future<ClientChannel>>> registerChannels = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Promise<ClientChannel>> ChannelOfPromise = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Future<MessageRouter>> routers = new ConcurrentHashMap<>();
    private final ClientConfig config;
    private final CombineListener listener;
    private final List<SocketAddress> bootstrapAddress;
    protected String name;
    protected EventLoopGroup workerGroup;
    protected EventExecutor refreshMetadataExecutor;
    private Bootstrap bootstrap;
    private volatile boolean state = false;

    public Client(String name, ClientConfig config, CombineListener listener) {
        this.name = name;
        this.config = Objects.requireNonNull(config, "Client config not found");
        this.listener = Objects.requireNonNull(listener, "Client lister not found");
        this.bootstrapAddress = NetworkUtil.switchSocketAddress(config.getBootstrapAddresses());
    }

    private SocketAddress bootstrapAddress() {
        int size = bootstrapAddress.size();
        if (size == 0) {
            return null;
        }

        if (size == 1) {
            return bootstrapAddress.getFirst();
        }

        return bootstrapAddress.get(ThreadLocalRandom.current().nextInt(size));
    }

    public ClientChannel getActiveChannel(SocketAddress address) {
        try {
            return applyChannel(address).get(config.getChannelConnectionTimeoutMilliseconds(), TimeUnit.MILLISECONDS);
        } catch (Throwable t) {
            if (address == null) {
                throw new MeteorClientException(
                        STR."Client[\{name}] fetch random channel failed, socket address cannot be empty", t);
            }
            throw new MeteorClientException(
                    STR."Client[\{name}] fetch random client channel failed, address[\{address}]", t);
        }
    }

    public ConcurrentMap<String, Future<MessageRouter>> getRouters() {
        return routers;
    }

    @Nonnull
    private Future<ClientChannel> applyChannel(SocketAddress address) {
        Future<ClientChannel> future;
        if (address == null) {
            future = randomAcquire();
            if (future != null) {
                return future;
            }

            address = bootstrapAddress();
            if (address == null) {
                throw new IllegalArgumentException(STR."Client[\{name}] Bootstrap address not found");
            }
        }

        List<Future<ClientChannel>> futures = filter(address);
        if (futures != null && futures.size() >= config.getConnectionPoolCapacity()) {
            return futures.get(ThreadLocalRandom.current().nextInt(futures.size()));
        }

        synchronized (registerChannels) {
            futures = filter(address);
            if (futures != null && futures.size() >= config.getConnectionPoolCapacity()) {
                return futures.get(ThreadLocalRandom.current().nextInt(futures.size()));
            }

            future = channelFuture(address);
            registerChannels.computeIfAbsent(address, k -> new CopyOnWriteArrayList<>()).add(future);
            final SocketAddress theAddress = address;
            future.addListener((GenericFutureListener<Future<ClientChannel>>) f -> {
                if (!f.isSuccess()) {
                    removeChannel(theAddress, f);
                } else {
                    f.getNow().onClosed(() -> removeChannel(theAddress, f));
                }
            });

            return future;
        }
    }

    private void removeChannel(SocketAddress address, Future<ClientChannel> future) {
        synchronized (registerChannels) {
            List<Future<ClientChannel>> futures = registerChannels.get(address);
            if (futures == null) {
                return;
            }

            futures.remove(future);
            if (futures.isEmpty()) {
                registerChannels.remove(address);
            }
        }
    }

    public Future<ClientChannel> channelFuture(SocketAddress address) {
        Bootstrap theBootstrap = bootstrap.clone().handler(new InternalChannelInitializer(address, config, listener, ChannelOfPromise));
        ChannelFuture channelFuture = theBootstrap.connect(address);
        Channel channel = channelFuture.channel();
        Promise<ClientChannel> assemblePromise = ChannelOfPromise
                .computeIfAbsent(channel.id().asLongText(), k -> ImmediateEventExecutor.INSTANCE.newPromise());
        channelFuture.addListener(future -> {
            if (!future.isSuccess()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Client[{}] remote channel[{}]", name, channel);
                }
                assemblePromise.tryFailure(future.cause());
                ChannelOfPromise.remove(channel.id().asLongText());
            }
        });

        channel.closeFuture().addListener(f -> {
            assemblePromise.tryFailure(
                    new IllegalArgumentException(STR."Client[\{name}] channel[\{channel}] was closed"));
            ChannelOfPromise.remove(channel.id().asLongText());
        });

        return assemblePromise;
    }

    private List<Future<ClientChannel>> filter(SocketAddress address) {
        List<Future<ClientChannel>> futures = registerChannels.get(address);
        return futures == null ? null : futures.stream().filter(this::isReady).collect(Collectors.toList());
    }

    private boolean isReady(Future<ClientChannel> future) {
        if (future == null) {
            return false;
        }

        if (!future.isDone()) {
            return true;
        }

        if (!future.isSuccess()) {
            return false;
        }

        ClientChannel channel = future.getNow();
        return channel != null && channel.isActive();
    }

    private Future<ClientChannel> randomAcquire() {
        int size = registerChannels.size();
        if (size == 0) {
            return null;
        }
        List<Future<ClientChannel>> validChannels = registerChannels.values().stream()
                .flatMap(Collection::stream)
                .filter(this::isReady).toList();

        if (validChannels.isEmpty()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Client[{}] valid channel is empty", name);
            }
            return null;
        }

        if (validChannels.size() == 1) {
            return validChannels.getFirst();
        }

        return validChannels.get(ThreadLocalRandom.current().nextInt(validChannels.size()));
    }

    public boolean isRunning() {
        return state;
    }

    public synchronized void start() {
        if (isRunning()) {
            if (logger.isWarnEnabled()) {
                logger.warn("Client[{}] is running, don't run it replay", name);
            }
            return;
        }

        state = true;
        workerGroup = newEventLoopGroup(config.isSocketEpollPrefer(), config.getWorkerThreadLimit(),
                STR."client-worker(\{name})", false);
        DnsNameResolverBuilder builder = new DnsNameResolverBuilder();
        builder.ttl(30, 300);
        builder.negativeTtl(30);

        if (config.isSocketEpollPrefer() && Epoll.isAvailable()) {
            builder.channelType(EpollDatagramChannel.class);
        } else {
            builder.channelType(NioDatagramChannel.class);
        }

        builder.nameServerProvider(DefaultDnsServerAddressStreamProvider.INSTANCE);
        bootstrap = new Bootstrap()
                .group(workerGroup)
                .channel(NetworkUtil.preferChannelClass(config.isSocketEpollPrefer()))
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, false)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getChannelConnectionTimeoutMilliseconds())
                .option(ChannelOption.SO_SNDBUF, config.getSocketSendBufferSize())
                .option(ChannelOption.SO_RCVBUF, config.getSocketReceiveBufferSize())
                .resolver(new RoundRobinDnsAddressResolverGroup(builder));
        for (SocketAddress address : bootstrapAddress) {
            applyChannel(address);
        }

        refreshMetadataExecutor = new DefaultEventExecutor(new DefaultThreadFactory(STR."client(\{name})-task"));
        refreshMetadataExecutor.schedule(new RefreshMetadataTask(this, config), config.getMetadataRefreshPeriodMilliseconds(), TimeUnit.MILLISECONDS);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public synchronized void close() {
        if (!isRunning()) {
            if (logger.isWarnEnabled()) {
                logger.warn("Client[{}] was closed, don't execute it replay", name);
            }
            return;
        }

        state = false;
        if (refreshMetadataExecutor != null) {
            Future<?> future = refreshMetadataExecutor.shutdownGracefully();
            future.addListener(f -> {
                if (workerGroup != null) {
                    workerGroup.shutdownGracefully().sync();
                }
            });

            try {
                while (!future.isDone()) {
                    future.wait(Integer.MAX_VALUE);
                }

                while (!refreshMetadataExecutor.isTerminated()) {
                    refreshMetadataExecutor.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
                }
            } catch (Exception e) {
                // Let the caller handle the interruption.
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void bindTo(@Nonnull MeterRegistry meterRegistry) {
        SingleThreadEventExecutor refreshExecutor = (SingleThreadEventExecutor) refreshMetadataExecutor;
        registerMetrics("client-refresh-task", meterRegistry, refreshExecutor);

        for (EventExecutor eventExecutor : workerGroup) {
            SingleThreadEventExecutor workerExecutor = (SingleThreadEventExecutor) eventExecutor;
            registerMetrics("client-worker", meterRegistry, workerExecutor);
        }
    }

    private void registerMetrics(String type, MeterRegistry meterRegistry, SingleThreadEventExecutor executor) {
        Gauge.builder(CLIENT_NETTY_PENDING_TASK_NAME, executor, SingleThreadEventExecutor::pendingTasks)
                .tag("type", type)
                .tag("name", name)
                .tag("id", executor.threadProperties().name())
                .register(meterRegistry);
    }

    public MessageRouter fetchRouter(String topic) {
        ClientChannel channel = null;
        try {
            channel = getActiveChannel(null);
            return applyRouter(topic, channel, true).get();
        } catch (Throwable t) {
            throw new RuntimeException(
                    STR."Client[\{name}] fetch message router from given client channel[\{channel == null ? null :
                            channel.channel().remoteAddress()}] failed, topic[\{topic}]", t);
        }
    }

    public void refreshRouter(String topic, ClientChannel channel) {
        try {
            if (channel == null) {
                channel = getActiveChannel(null);
            }
            applyRouter(topic, channel, false).get();
        } catch (Throwable t) {
            throw new MeteorClientException(
                    STR."Client[\{name}] fetch message router from given client channel[\{channel == null ? null :
                            channel.channel().remoteAddress()}] failed, topic[\{topic}]", t);
        }
    }

    public boolean containsRouter(String topic) {
        return routers.containsKey(topic);
    }

    private Future<MessageRouter> applyRouter(String topic, ClientChannel channel, boolean useCached) {
        if (useCached) {
            Future<MessageRouter> result = routers.get(topic);
            if (result != null && (!result.isDone() || result.isSuccess())) {
                return result;
            }
        }

        Promise<MessageRouter> promise;
        synchronized (routers) {
            Future<MessageRouter> result = routers.get(topic);
            if (useCached && result != null && (!result.isDone() || result.isSuccess())) {
                return result;
            }

            promise = ImmediateEventExecutor.INSTANCE.newPromise();
            promise.addListener((GenericFutureListener<Future<MessageRouter>>) f -> {
                if (!f.isSuccess()) {
                    routers.remove(topic, f);
                }
            });
            if (result == null || (result.isDone() && (!result.isSuccess() || result.getNow() == null))) {
                routers.put(topic, promise);
            }
        }

        try {
            promise.trySuccess(cachingRouter(topic, queryRouter(channel, topic)));
        } catch (Throwable t) {
            promise.tryFailure(t);
        }
        return promise;
    }

    private MessageRouter queryRouter(ClientChannel channel, String topic) throws Exception {
        if (!channel.isActive()) {
            throw new IllegalStateException(STR."Client[\{name}] channel[\{channel}] is inactive");
        }

        ClusterInfo clusterInfo = queryClusterInfo(channel);
        if (clusterInfo == null) {
            throw new IllegalStateException(STR."Client[\{name}] that Cluster info not found");
        }

        TopicInfo topicInfo = queryTopicInfos(channel, topic).get(topic);
        if (topicInfo == null) {
            return null;
        }
        return buildRouter(topic, clusterInfo, topicInfo);
    }

    MessageRouter buildRouter(String topic, ClusterInfo clusterInfo, TopicInfo topicInfo) {
        TopicMetadata topicMetadata = topicInfo.hasTopic() ? topicInfo.getTopic() : null;
        if (topicMetadata == null) {
            return null;
        }

        Map<String, NodeMetadata> nodes = clusterInfo.getNodesMap();
        Map<Integer, MessageLedger> ledgers = new HashMap<>();
        for (PartitionMetadata partition : topicInfo.getPartitionsMap().values()) {
            int ledgerId = partition.getLedger();
            SocketAddress leaderAddress = null;
            NodeMetadata leaderNode = nodes.get(partition.getLeaderNodeId());
            if (leaderNode != null) {
                leaderAddress = NetworkUtil.switchSocketAddress(leaderNode.getHost(), leaderNode.getPort());
            }

            List<SocketAddress> replicaAddress = new ArrayList<>();
            for (String nodeId : partition.getReplicaNodeIdsList()) {
                NodeMetadata replicaNode = nodes.get(nodeId);
                if (replicaNode != null) {
                    SocketAddress address = NetworkUtil.switchSocketAddress(replicaNode.getHost(), replicaNode.getPort());
                    if (address != null) {
                        replicaAddress.add(address);
                    }
                }
            }

            Collections.shuffle(replicaAddress);
            MessageLedger ledger = new MessageLedger(ledgerId, partition.getVersion(), leaderAddress, replicaAddress, partition.getTopicName(), partition.getId());
            ledgers.put(ledgerId, ledger);
        }
        long token = (((long) topicMetadata.getId() << 32 | topicMetadata.getVersion()));
        return new MessageRouter(token, topic, ledgers);
    }

    private MessageRouter combineRouter(MessageRouter cacheRouter, MessageRouter router) {
        if (cacheRouter == null) {
            return router;
        }

        if (router == null) {
            return null;
        }

        if (cacheRouter.token() != router.token()) {
            return cacheRouter.token() > router.token() ? cacheRouter : router;
        }
        Map<Integer, MessageLedger> ledgers = new HashMap<>();
        for (MessageLedger messageLedger : router.ledgers().values()) {
            int ledgerId = messageLedger.id();
            MessageLedger cachedLedger = cacheRouter.ledger(ledgerId);
            if (cachedLedger != null && cachedLedger.version() > messageLedger.version()) {
                ledgers.put(ledgerId, cachedLedger);
            } else {
                ledgers.put(ledgerId, messageLedger);
            }
        }
        return new MessageRouter(router.token(), router.topic(), ledgers);
    }

    MessageRouter cachingRouter(String topic, MessageRouter router) {
        synchronized (routers) {
            Future<MessageRouter> future = routers.get(topic);
            if (future != null && future.isDone() && future.isSuccess()) {
                router = combineRouter(future.getNow(), router);
            }

            future = new SucceededFuture<>(ImmediateEventExecutor.INSTANCE, router);
            routers.put(topic, future);
            return router;
        }
    }

    public ClusterInfo queryClusterInfo(ClientChannel channel) throws Exception {
        QueryClusterInfoRequest request = QueryClusterInfoRequest.newBuilder().build();
        Promise<QueryClusterResponse> promise = ImmediateEventExecutor.INSTANCE.newPromise();
        channel.invoker().queryClusterInfo(config.getMetadataTimeoutMilliseconds(), promise, request);

        QueryClusterResponse response = promise.get(config.getMetadataTimeoutMilliseconds(), TimeUnit.MILLISECONDS);
        return response.hasClusterInfo() ? response.getClusterInfo() : null;
    }

    public Map<String, TopicInfo> queryTopicInfos(ClientChannel channel, String... topics) throws Exception {
        QueryTopicInfoRequest request = QueryTopicInfoRequest.newBuilder()
                .addAllTopicNames(Arrays.asList(topics))
                .build();

        Promise<QueryTopicInfoResponse> promise = ImmediateEventExecutor.INSTANCE.newPromise();
        channel.invoker().queryTopicInfo(config.getMetadataTimeoutMilliseconds(), promise, request);

        return promise.get(config.getMetadataTimeoutMilliseconds(), TimeUnit.MILLISECONDS).getTopicInfosMap();
    }

    public CreateTopicResponse createTopic(String topic, int partitions, int replicas) throws Exception {
        return createTopic(topic, partitions, replicas, null);
    }

    public CreateTopicResponse createTopic(String topic, int partitions, int replicas, TopicConfig topicConfig) throws Exception {
        TopicPatternUtil.validatePartition(partitions);
        TopicPatternUtil.validateTopic(topic);
        TopicPatternUtil.validateLedgerReplica(replicas);

        CreateTopicRequest.Builder request = CreateTopicRequest.newBuilder()
                .setTopic(topic)
                .setPartition(partitions)
                .setReplicas(replicas);

        if (topicConfig != null) {
            CreateTopicConfigRequest.Builder cr = CreateTopicConfigRequest.newBuilder();
            cr.setSegmentRetainCount(topicConfig.getSegmentRetainCount());
            cr.setSegmentRollingSize(topicConfig.getSegmentRollingSize());
            cr.setSegmentRetainMs(topicConfig.getSegmentRetainMs());
            cr.setAllocate(topicConfig.isAllocate());
            cr.build();
            request.setConfigs(cr);
        }

        Promise<CreateTopicResponse> promise = ImmediateEventExecutor.INSTANCE.newPromise();
        ClientChannel channel = getActiveChannel(null);
        channel.invoker().createTopic(config.getCreateTopicTimeoutMilliseconds(), promise, request.build());
        return promise.get(config.getCreateTopicTimeoutMilliseconds(), TimeUnit.MILLISECONDS);
    }

    public DeleteTopicResponse deleteTopic(String topic) throws Exception {
        TopicPatternUtil.validateTopic(topic);

        DeleteTopicRequest request = DeleteTopicRequest.newBuilder().setTopic(topic).build();
        Promise<DeleteTopicResponse> promise = ImmediateEventExecutor.INSTANCE.newPromise();
        ClientChannel channel = getActiveChannel(null);
        channel.invoker().deleteTopic(config.getDeleteTopicTimeoutMilliseconds(), promise, request);
        return promise.get(config.getDeleteTopicTimeoutMilliseconds(), TimeUnit.MILLISECONDS);
    }

    public CalculatePartitionsResponse calculatePartitions() throws Exception {
        ClientChannel clientChannel = getActiveChannel(null);
        Promise<CalculatePartitionsResponse> promise = ImmediateEventExecutor.INSTANCE.newPromise();
        CalculatePartitionsRequest request = CalculatePartitionsRequest.newBuilder().build();
        clientChannel.invoker().calculatePartitions(config.getCalculatePartitionsTimeoutMilliseconds(), promise, request);
        return promise.get(config.getCalculatePartitionsTimeoutMilliseconds(), TimeUnit.MILLISECONDS);
    }

    public MigrateLedgerResponse migrateLedger(String topic, int partition, String original, String destination) throws Exception {
        MigrateLedgerResponse.Builder response = MigrateLedgerResponse.newBuilder();
        ClientChannel clientChannel = getActiveChannel(null);
        ClusterInfo clusterInfo = queryClusterInfo(clientChannel);
        Map<String, NodeMetadata> nodesMap = clusterInfo.getNodesMap();
        NodeMetadata originalBroker = nodesMap.get(original);
        if (originalBroker == null) {
            return response.setSuccess(false)
                    .setMessage(STR."The client[\{name}] that original broker[\{original}] is not in cluster").build();
        }

        if (!nodesMap.containsKey(destination)) {
            return response.setSuccess(false)
                    .setMessage(STR."The client[\{name}] that destination broker[\{original}] is not in cluster")
                    .build();
        }

        Map<String, TopicInfo> topicInfos = queryTopicInfos(clientChannel, topic);
        if (topicInfos == null || topicInfos.isEmpty()) {
            return response.setSuccess(false).setMessage(STR."The client[\{name}] of topic[\{topic}] does not exist")
                    .build();
        }
        TopicInfo topicInfo = topicInfos.get(topic);
        PartitionMetadata partitionMetadata = topicInfo.getPartitionsMap().get(partition);
        if (partitionMetadata == null) {
            return response.setSuccess(false)
                    .setMessage(STR."The client[\{name}] of topic[\{topic}] and partition[\{partition}] are not exist")
                    .build();
        }

        clientChannel = getActiveChannel(new InetSocketAddress(originalBroker.getHost(), originalBroker.getPort()));
        Promise<MigrateLedgerResponse> promise = ImmediateEventExecutor.INSTANCE.newPromise();
        MigrateLedgerRequest request = MigrateLedgerRequest.newBuilder()
                .setTopic(topic)
                .setPartition(partition)
                .setOriginal(original)
                .setDestination(destination)
                .build();

        clientChannel.invoker().migrateLedger(config.getMigrateLedgerTimeoutMilliseconds(), promise, request);
        return promise.get(config.getMigrateLedgerTimeoutMilliseconds(), TimeUnit.MILLISECONDS);
    }
}
