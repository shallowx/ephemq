package org.ostara.client.internal;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollDatagramChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.resolver.dns.DefaultDnsServerAddressStreamProvider;
import io.netty.resolver.dns.DnsNameResolverBuilder;
import io.netty.resolver.dns.RoundRobinDnsAddressResolverGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.*;
import org.ostara.client.util.TopicPatterns;
import org.ostara.common.TopicConfig;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.remote.RemoteException;
import org.ostara.remote.codec.MessageDecoder;
import org.ostara.remote.codec.MessageEncoder;
import org.ostara.remote.handle.ConnectDuplexHandler;
import org.ostara.remote.handle.ProcessDuplexHandler;
import org.ostara.remote.invoke.InvokeAnswer;
import org.ostara.remote.processor.ProcessCommand;
import org.ostara.remote.processor.Processor;
import org.ostara.remote.proto.*;
import org.ostara.remote.proto.client.MessagePushSignal;
import org.ostara.remote.proto.client.NodeOfflineSignal;
import org.ostara.remote.proto.client.SyncMessageSignal;
import org.ostara.remote.proto.client.TopicChangedSignal;
import org.ostara.remote.proto.server.*;
import org.ostara.remote.util.NetworkUtils;
import org.ostara.remote.util.ProtoBufUtils;

import javax.annotation.Nonnull;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class Client implements MeterBinder {
    protected static final String CLIENT_NETTY_PENDING_TASK_NAME = "client_netty_pending_task";
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(Client.class);
    private final ClientConfig config;
    private final ClientListener listener;
    private final List<SocketAddress> bootstrapAddress;
    private final Map<SocketAddress, List<Future<ClientChannel>>> channels = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Promise<ClientChannel>> assembleChannels = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Future<MessageRouter>> routers = new ConcurrentHashMap<>();
    protected String name;
    protected EventLoopGroup workerGroup;
    protected EventExecutor taskExecutor;
    private Bootstrap bootstrap;
    private volatile Boolean state;

    public Client(String name, ClientConfig config, ClientListener listener) {
        this.name = name;
        this.config = Objects.requireNonNull(config, "client config not found");
        this.listener = Objects.requireNonNull(listener, "client lister not found");
        this.bootstrapAddress = NetworkUtils.switchSocketAddress(config.getBootstrapAddresses());
    }

    private SocketAddress bootstrapAddress() {
        int size = bootstrapAddress.size();
        if (size == 0) {
            return null;
        }

        if (size == 1) {
            return bootstrapAddress.get(0);
        }

        return bootstrapAddress.get(ThreadLocalRandom.current().nextInt(size));
    }

    public ClientChannel fetchChannel(SocketAddress address) {
        try {
            return acquireChannel(address).get(config.getChannelConnectionTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (Throwable t) {
            if (address == null) {
                throw new RuntimeException("Fetch random client channel failed", t);
            }
            throw new RuntimeException(String.format("Fetch random client channel failed, address=%s", address), t);
        }
    }

    @Nonnull
    private Future<ClientChannel> acquireChannel(SocketAddress address) {
        Future<ClientChannel> future;
        if (address == null) {
            future = randomAcquire();
            if (future != null) {
                return future;
            }

            address = bootstrapAddress();
            if (address == null) {
                throw new IllegalArgumentException("Bootstrap address not found");
            }
        }

        List<Future<ClientChannel>> futures = listValidChannels(address);
        if (futures != null && futures.size() >= config.getConnectionPoolCapacity()) {
            return futures.get(ThreadLocalRandom.current().nextInt(futures.size()));
        }

        synchronized (channels) {
            futures = listValidChannels(address);
            if (futures != null && futures.size() >= config.getConnectionPoolCapacity()) {
                return futures.get(ThreadLocalRandom.current().nextInt(futures.size()));
            }

            future = createChannelFuture(address);
            channels.computeIfAbsent(address, k -> new CopyOnWriteArrayList<>()).add(future);
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
        synchronized (channels) {
            List<Future<ClientChannel>> futures = channels.get(address);
            if (futures == null) {
                return;
            }

            futures.remove(future);
            if (futures.isEmpty()) {
                channels.remove(address);
            }
        }
    }

    public Future<ClientChannel> createChannelFuture(SocketAddress address) {
        Bootstrap theBootstrap = bootstrap.clone().handler(new InnerChannelInitializer(address));
        ChannelFuture channelFuture = theBootstrap.connect(address);
        Channel channel = channelFuture.channel();
        Promise<ClientChannel> assemblePromise = assembleChannels
                .computeIfAbsent(channel.id().asLongText(), k -> ImmediateEventExecutor.INSTANCE.newPromise());
        channelFuture.addListener(future -> {
            if (!future.isSuccess()) {
                assemblePromise.tryFailure(future.cause());
                assembleChannels.remove(channel.id().asLongText());
            }
        });

        channel.closeFuture().addListener(f -> {
            assemblePromise.tryFailure(new IllegalArgumentException("Client channel is closed"));
            assembleChannels.remove(channel.id().asLongText());
        });

        return assemblePromise;
    }

    private List<Future<ClientChannel>> listValidChannels(SocketAddress address) {
        List<Future<ClientChannel>> futures = channels.get(address);
        return futures == null ? null : futures.stream().filter(this::isValidChannel).collect(Collectors.toList());
    }

    private boolean isValidChannel(Future<ClientChannel> future) {
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
        int size = channels.size();
        if (size == 0) {
            return null;
        }
        List<Future<ClientChannel>> validChannels = channels.values().stream()
                .flatMap(Collection::stream)
                .filter(this::isValidChannel).toList();

        if (validChannels.isEmpty()) {
            return null;
        }

        if (validChannels.size() == 1) {
            return validChannels.get(0);
        }

        return validChannels.get(ThreadLocalRandom.current().nextInt(validChannels.size()));
    }

    public boolean isRunning() {
        return state != null && state;
    }

    public synchronized void start() {
        if (isRunning()) {
            return;
        }

        state = Boolean.TRUE;
        workerGroup = NetworkUtils.newEventLoopGroup(config.isSocketEpollPrefer(), config.getWorkerThreadCount(), "client-worker(" + name + ")");
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
                .channel(NetworkUtils.preferChannelClass(config.isSocketEpollPrefer()))
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, false)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getChannelConnectionTimeoutMs())
                .option(ChannelOption.SO_SNDBUF, config.getSocketSendBufferSize())
                .option(ChannelOption.SO_RCVBUF, config.getSocketReceiveBufferSize())
                .resolver(new RoundRobinDnsAddressResolverGroup(builder));
        for (SocketAddress address : bootstrapAddress) {
            acquireChannel(address);
        }

        taskExecutor = new DefaultEventExecutor(new DefaultThreadFactory("client(" + name + ")-task"));
        taskExecutor.schedule(new RefreshMetadataTask(), config.getMetadataRefreshPeriodMs(), TimeUnit.MILLISECONDS);
    }

    public synchronized void close() {
        if (!isRunning()) {
            return;
        }

        state = Boolean.FALSE;
        if (taskExecutor != null) {
            Future<?> future = taskExecutor.shutdownGracefully();
            future.addListener(f -> {
                if (workerGroup != null) {
                    workerGroup.shutdownGracefully();
                }
            });
        }
    }

    @Override
    public void bindTo(MeterRegistry meterRegistry) {
        {
            SingleThreadEventExecutor executor = (SingleThreadEventExecutor) taskExecutor;
            Gauge.builder(CLIENT_NETTY_PENDING_TASK_NAME, executor, SingleThreadEventExecutor::pendingTasks)
                    .tag("type", "client-task")
                    .tag("name", name)
                    .tag("id", executor.threadProperties().name())
                    .register(meterRegistry);
        }

        for (EventExecutor eventExecutor : workerGroup) {
            SingleThreadEventExecutor executor = (SingleThreadEventExecutor) eventExecutor;
            Gauge.builder(CLIENT_NETTY_PENDING_TASK_NAME, executor, SingleThreadEventExecutor::pendingTasks)
                    .tag("type", "client-worker")
                    .tag("name", name)
                    .tag("id", executor.threadProperties().name())
                    .register(meterRegistry);
        }
    }

    protected ClientChannel createClientChannel(ClientConfig clientConfig, Channel channel, SocketAddress address) {
        return new ClientChannel(clientConfig, channel, address);
    }

    private void onSyncMessage(ClientChannel channel, ByteBuf data, InvokeAnswer<ByteBuf> answer) throws Exception {
        SyncMessageSignal signal = ProtoBufUtils.readProto(data, SyncMessageSignal.parser());
        listener.onSyncMessage(channel, signal, data);
        if (answer != null) {
            answer.success(null);
        }
    }

    private void onTopicChanged(ClientChannel channel, ByteBuf data, InvokeAnswer<ByteBuf> answer) throws Exception {
        TopicChangedSignal signal = ProtoBufUtils.readProto(data, TopicChangedSignal.parser());
        listener.onTopicChanged(channel, signal);
        if (answer != null) {
            answer.success(null);
        }
    }

    private void onNodeOffline(ClientChannel channel, ByteBuf data, InvokeAnswer<ByteBuf> answer) throws Exception {
        NodeOfflineSignal signal = ProtoBufUtils.readProto(data, NodeOfflineSignal.parser());
        listener.onNodeOffline(channel, signal);
        if (answer != null) {
            answer.success(null);
        }
    }

    private void onPushMessage(ClientChannel channel, ByteBuf data, InvokeAnswer<ByteBuf> answer) throws Exception {
        MessagePushSignal signal = ProtoBufUtils.readProto(data, MessagePushSignal.parser());
        listener.onPushMessage(channel, signal, data);
        if (answer != null) {
            answer.success(null);
        }
    }

    public MessageRouter fetchMessageRouter(String topic) {
        ClientChannel channel = null;
        try {
            channel = fetchChannel(null);
            return applyMessageRouter(topic, channel, true).get();
        } catch (Throwable t) {
            throw new RuntimeException(
                    String.format(
                            "Fetch message router from given client channel %s failed, topic=%s",
                            channel == null ? null : channel.channel().remoteAddress(), topic
                    ), t
            );
        }
    }

    public MessageRouter refreshMessageRouter(String topic, ClientChannel channel) {
        try {
            if (channel == null) {
                channel = fetchChannel(null);
            }
            return applyMessageRouter(topic, channel, false).get();
        } catch (Throwable t) {
            throw new RuntimeException(
                    String.format(
                            "Refresh message router from given client channel %s failed, topic=%s",
                            channel == null ? null : channel.channel().remoteAddress(), topic
                    ), t
            );
        }
    }

    public boolean containsMessageRouter(String topic) {
        return routers.containsKey(topic);
    }

    private Future<MessageRouter> applyMessageRouter(String topic, ClientChannel channel, boolean useCached) {
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
            promise.trySuccess(cachingMessageRouter(topic, queryMessageRouter(channel, topic)));
        } catch (Throwable t) {
            promise.tryFailure(t);
        }
        return promise;
    }

    private MessageRouter queryMessageRouter(ClientChannel channel, String topic) throws Exception {
        if (!channel.isActive()) {
            throw new IllegalStateException("Client channel is inactive");
        }

        ClusterInfo clusterInfo = queryClusterInfo(channel);
        if (clusterInfo == null) {
            throw new IllegalStateException("Cluster info not found");
        }

        TopicInfo topicInfo = queryTopicInfos(channel, topic).get(topic);
        if (topicInfo == null) {
            return null;
        }
        return assembleMessageRouter(topic, clusterInfo, topicInfo);
    }

    private MessageRouter assembleMessageRouter(String topic, ClusterInfo clusterInfo, TopicInfo topicInfo) {
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
                leaderAddress = NetworkUtils.switchSocketAddress(leaderNode.getHost(), leaderNode.getPort());
            }

            List<SocketAddress> replicaAddress = new ArrayList<>();
            for (String nodeId : partition.getReplicaNodeIdsList()) {
                NodeMetadata replicaNode = nodes.get(nodeId);
                if (replicaNode != null) {
                    SocketAddress address = NetworkUtils.switchSocketAddress(replicaNode.getHost(), replicaNode.getPort());
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

    private MessageRouter mergeMessageRouter(MessageRouter cacheRouter, MessageRouter queryRouter) {
        if (cacheRouter == null) {
            return queryRouter;
        }

        if (queryRouter == null) {
            return null;
        }

        if (cacheRouter.token() != queryRouter.token()) {
            return cacheRouter.token() > queryRouter.token() ? cacheRouter : queryRouter;
        }
        Map<Integer, MessageLedger> ledgers = new HashMap<>();
        for (MessageLedger queryLedger : queryRouter.ledgers().values()) {
            int ledgerId = queryLedger.id();
            MessageLedger cachedLedger = cacheRouter.ledger(ledgerId);
            if (cachedLedger != null && cachedLedger.version() > queryLedger.version()) {
                ledgers.put(ledgerId, cachedLedger);
            } else {
                ledgers.put(ledgerId, queryLedger);
            }
        }
        return new MessageRouter(queryRouter.token(), queryRouter.topic(), ledgers);
    }

    private MessageRouter cachingMessageRouter(String topic, MessageRouter router) {
        synchronized (routers) {
            Future<MessageRouter> future = routers.get(topic);
            if (future != null && future.isDone() && future.isSuccess()) {
                router = mergeMessageRouter(future.getNow(), router);
            }

            future = new SucceededFuture<>(ImmediateEventExecutor.INSTANCE, router);
            routers.put(topic, future);
            return router;
        }
    }

    public ClusterInfo queryClusterInfo(ClientChannel channel) throws Exception {
        try {
            QueryClusterInfoRequest request = QueryClusterInfoRequest.newBuilder().build();
            Promise<QueryClusterResponse> promise = ImmediateEventExecutor.INSTANCE.newPromise();
            channel.invoker().queryClusterInfo(config.getMetadataTimeoutMs(), promise, request);

            QueryClusterResponse response = promise.get(config.getMetadataTimeoutMs(), TimeUnit.MILLISECONDS);
            return response.hasClusterInfo() ? response.getClusterInfo() : null;
        } catch (Exception e) {
            throw e;
        }
    }

    public Map<String, TopicInfo> queryTopicInfos(ClientChannel channel, String... topics) throws Exception {
        try {
            QueryTopicInfoRequest request = QueryTopicInfoRequest.newBuilder()
                    .addAllTopicNames(Arrays.asList(topics))
                    .build();

            Promise<QueryTopicInfoResponse> promise = ImmediateEventExecutor.INSTANCE.newPromise();
            channel.invoker().queryTopicInfo(config.getMetadataTimeoutMs(), promise, request);

            return promise.get(config.getMetadataTimeoutMs(), TimeUnit.MILLISECONDS).getTopicInfosMap();
        } catch (Exception e) {
            throw e;
        }
    }

    private void refreshMetadata() {
        Set<String> topics = new HashSet<>();
        for (Map.Entry<String, Future<MessageRouter>> entry : routers.entrySet()) {
            String topic = entry.getKey();
            Future<MessageRouter> future = entry.getValue();
            if (future.isDone() && future.isSuccess() && future.getNow() == null) {
                routers.remove(topic, future);
                continue;
            }

            topics.add(topic);
        }

        if (topics.isEmpty()) {
            return;
        }

        Map<String, MessageRouter> queryRouters = new HashMap<>();
        try {
            ClientChannel channel = fetchChannel(null);
            ClusterInfo clusterInfo = queryClusterInfo(channel);
            if (clusterInfo == null) {
                throw new IllegalStateException("Cluster node not found");
            }

            Map<String, TopicInfo> topicInfos = queryTopicInfos(channel, topics.toArray(new String[0]));
            for (String topic : topics) {
                TopicInfo topicInfo = topicInfos.get(topic);
                if (topicInfo == null) {
                    continue;
                }
                MessageRouter router = assembleMessageRouter(topic, clusterInfo, topicInfo);
                if (router == null) {
                    continue;
                }
                queryRouters.put(topic, router);
            }
        } catch (Throwable ignored) {
        }

        if (queryRouters.isEmpty()) {
            return;
        }

        for (Map.Entry<String, MessageRouter> entry : queryRouters.entrySet()) {
            cachingMessageRouter(entry.getKey(), entry.getValue());
        }
    }

    public CreateTopicResponse createTopic(String topic, int partitions, int replicas) throws Exception {
        return createTopic(topic, partitions, replicas, null);
    }

    public CreateTopicResponse createTopic(String topic, int partitions, int replicas, TopicConfig topicConfig) throws Exception {
        TopicPatterns.validatePartition(partitions);
        TopicPatterns.validateTopic(topic);
        TopicPatterns.validateLedgerReplica(replicas);

        CreateTopicRequest.Builder request = CreateTopicRequest.newBuilder()
                .setTopic(topic)
                .setPartition(partitions)
                .setReplicas(replicas);

        if (topicConfig != null) {
            CreateTopicConfigRequest.Builder cr = CreateTopicConfigRequest.newBuilder();
            cr.setSegmentRetainCount(topicConfig.getSegmentRetainCount());
            cr.setSegmentRollingSize(topicConfig.getSegmentRollingSize());
            cr.setSegmentRetainMs(topicConfig.getSegmentRetainMs());

            cr.build();

            request.setConfigs(cr);
        }

        Promise<CreateTopicResponse> promise = ImmediateEventExecutor.INSTANCE.newPromise();
        ClientChannel channel = fetchChannel(null);
        channel.invoker().createTopic(config.getCreateTopicTimeoutMs(), promise, request.build());

        return promise.get(config.getCreateTopicTimeoutMs(), TimeUnit.MILLISECONDS);
    }

    public DeleteTopicResponse deleteTopic(String topic) throws Exception {
        TopicPatterns.validateTopic(topic);

        DeleteTopicRequest request = DeleteTopicRequest.newBuilder().setTopic(topic).build();
        Promise<DeleteTopicResponse> promise = ImmediateEventExecutor.INSTANCE.newPromise();
        ClientChannel channel = fetchChannel(null);
        channel.invoker().deleteTopic(config.getDeleteTopicTimeoutMs(), promise, request);

        return promise.get(config.getDeleteTopicTimeoutMs(), TimeUnit.MILLISECONDS);
    }

    public CalculatePartitionsResponse calculatePartitions() throws Exception {
        ClientChannel clientChannel = fetchChannel(null);
        Promise<CalculatePartitionsResponse> promise = ImmediateEventExecutor.INSTANCE.newPromise();
        CalculatePartitionsRequest request = CalculatePartitionsRequest.newBuilder().build();
        clientChannel.invoker().calculatePartitions(config.getCalculatePartitionsTimeoutMs(), promise, request);
        return promise.get(config.getCalculatePartitionsTimeoutMs(), TimeUnit.MILLISECONDS);
    }

    public MigrateLedgerResponse migrateLedger(String topic, int partition, String original, String destination) throws Exception {
        MigrateLedgerResponse.Builder response = MigrateLedgerResponse.newBuilder();
        ClientChannel clientChannel = fetchChannel(null);
        ClusterInfo clusterInfo = queryClusterInfo(clientChannel);
        Map<String, NodeMetadata> nodesMap = clusterInfo.getNodesMap();
        NodeMetadata originalBroker = nodesMap.get(original);
        if (originalBroker == null) {
            return response.setSuccess(false).setMessage(String.format("The original broker %s is not in cluster", original)).build();
        }

        if (!nodesMap.containsKey(destination)) {
            return response.setSuccess(false).setMessage(String.format("The destination broker %s is not in cluster", original)).build();
        }

        Map<String, TopicInfo> topicInfos = queryTopicInfos(clientChannel, topic);
        if (topicInfos == null || topicInfos.isEmpty()) {
            return response.setSuccess(false).setMessage(String.format("The topic %s does not exist", original)).build();
        }
        TopicInfo topicInfo = topicInfos.get(topic);
        PartitionMetadata partitionMetadata = topicInfo.getPartitionsMap().get(partition);
        if (partitionMetadata == null) {
            return response.setSuccess(false).setMessage(String.format("The topic %s partition %d does not exist", original, partition)).build();
        }

        clientChannel = fetchChannel(new InetSocketAddress(originalBroker.getHost(), originalBroker.getPort()));
        Promise<MigrateLedgerResponse> promise = ImmediateEventExecutor.INSTANCE.newPromise();
        MigrateLedgerRequest request = MigrateLedgerRequest.newBuilder()
                .setTopic(topic)
                .setPartition(partition)
                .setOriginal(original)
                .setDestination(destination)
                .build();

        clientChannel.invoker().migrateLedger(config.getMigrateLedgerTimeoutMs(), promise, request);
        return promise.get(config.getMigrateLedgerTimeoutMs(), TimeUnit.MILLISECONDS);
    }

    private class RefreshMetadataTask implements Runnable {
        @Override
        public void run() {
            if (taskExecutor.isShuttingDown()) {
                return;
            }

            try {
                refreshMetadata();
            } catch (Throwable t) {
                String message = t.getMessage();
                if (message == null || message.isEmpty()) {
                    message = t.getClass().getName();
                }
                logger.error("Refresh metadata failed, {}", message);
            }

            if (!taskExecutor.isShuttingDown()) {
                taskExecutor.schedule(this, config.getMetadataRefreshPeriodMs(), TimeUnit.MILLISECONDS);
            }
        }
    }

    private class InnerChannelInitializer extends ChannelInitializer<SocketChannel> {

        private final SocketAddress address;

        public InnerChannelInitializer(SocketAddress address) {
            this.address = address;
        }

        @Override
        protected void initChannel(SocketChannel socketChannel) throws Exception {
            ClientChannel clientChannel = createClientChannel(config, socketChannel, address);
            socketChannel.pipeline()
                    .addLast("packet-encoder", MessageEncoder.instance())
                    .addLast("packet-decoder", new MessageDecoder())
                    .addLast("connect-handler", new ConnectDuplexHandler(
                            config.getChannelKeepPeriodMs(), config.getChannelIdleTimeoutMs()
                    ))
                    .addLast("service-handler", new ProcessDuplexHandler(new InnerServiceProcessor(clientChannel)));
        }

        private class InnerServiceProcessor implements Processor, ProcessCommand.Client {

            private final ClientChannel clientChannel;

            public InnerServiceProcessor(ClientChannel channel) {
                this.clientChannel = channel;
            }

            @Override
            public void onActive(Channel channel, EventExecutor executor) {
                try {
                    assembleChannels.computeIfAbsent(channel.id().asLongText(), k -> ImmediateEventExecutor.INSTANCE.newPromise()).setSuccess(clientChannel);
                    channel.closeFuture().addListener((ChannelFutureListener) f -> listener.onChannelClosed(clientChannel));
                    listener.onChannelActive(clientChannel);
                } catch (Throwable t) {
                    channel.close();
                }
            }

            @Override
            public void process(Channel channel, int command, ByteBuf data, InvokeAnswer<ByteBuf> answer) {
                int length = data.readableBytes();
                try {
                    switch (command) {
                        case ProcessCommand.Client.PUSH_MESSAGE -> onPushMessage(clientChannel, data, answer);
                        case ProcessCommand.Client.SERVER_OFFLINE -> onNodeOffline(clientChannel, data, answer);
                        case ProcessCommand.Client.TOPIC_INFO_CHANGED -> onTopicChanged(clientChannel, data, answer);
                        case ProcessCommand.Client.SYNC_MESSAGE -> onSyncMessage(clientChannel, data, answer);
                        default -> {
                            if (answer != null) {
                                answer.failure(RemoteException.of(ProcessCommand.Failure.COMMAND_EXCEPTION, "code unsupported: " + command));
                            }
                        }
                    }
                } catch (Throwable t) {
                    logger.error("<{}>Client<{}> processor is error, code={} length={}", NetworkUtils.switchAddress(clientChannel.channel()), name, command, length);
                    if (answer != null) {
                        answer.failure(t);
                    }
                }
            }
        }
    }
}
