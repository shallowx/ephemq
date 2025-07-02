package org.meteor.client.core;

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
import io.netty.util.concurrent.*;
import io.netty.util.concurrent.Future;
import org.meteor.client.exception.MeteorClientException;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.common.message.TopicConfig;
import org.meteor.common.util.TopicPatternUtil;
import org.meteor.remote.proto.*;
import org.meteor.remote.proto.server.*;
import org.meteor.remote.util.NetworkUtil;

import javax.annotation.Nonnull;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static org.meteor.client.core.MessageConstants.CLIENT_NETTY_PENDING_TASK_NAME;
import static org.meteor.remote.util.NetworkUtil.newEventLoopGroup;

/**
 * This class represents a client responsible for managing communication channels,
 * routers, and handling various client operations such as creating, deleting topics, etc.
 * It also integrates with Metrics for monitoring purposes.
 */
public class Client implements MeterBinder {
    /**
     * Logger instance for the Client class used to log information, warnings, and errors.
     * Utilizes an internal logging framework for handling the logging operations.
     */
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(Client.class);
    /**
     * A thread-safe map that keeps track of channels registered for each socket address.
     * The key of the map is a SocketAddress and the value is a list of futures representing
     * the client channels associated with that address.
     */
    private final Map<SocketAddress, List<Future<ClientChannel>>> registerChannels = new ConcurrentHashMap<>();
    /**
     * A thread-safe map that stores Promises associated with unique client channel identifiers.
     * It facilitates asynchronous operations by mapping a channel identifier (String) to a Promise
     * that resolves to a ClientChannel.
     */
    private final ConcurrentMap<String, Promise<ClientChannel>> ChannelOfPromise = new ConcurrentHashMap<>();
    /**
     * A thread-safe map that holds topic names as keys and their corresponding future {@link MessageRouter} instances as values.
     * <p>
     * This map is used to manage the lifecycle of message routers associated with different topics. The futures in the map
     * represent ongoing or completed asynchronous operations to retrieve or create {@link MessageRouter} objects.
     * <p>
     * The concurrency properties of the map ensure that multiple threads can safely access and modify the routers without
     * external synchronization.
     */
    private final ConcurrentMap<String, Future<MessageRouter>> routers = new ConcurrentHashMap<>();
    /**
     * The configuration object for the Client instance.
     * It holds various settings for the client's behavior and connections, such as timeout settings, buffer sizes,
     * and other networking configurations.
     */
    private final ClientConfig config;
    /**
     * A listener that combines various events and signals related to the client's channels.
     * It provides mechanisms to react to channel activities, such as:
     * - Channel activation and closure
     * - Push messages and topic changes
     * - Node offline signals and synchronization messages
     * - Completion of the listener
     */
    private final CombineListener listener;
    /**
     * A list of socket addresses used for initial connection setup.
     * These addresses are used to bootstrap the client and initiate communication
     * with a server or a cluster of servers.
     */
    private final List<SocketAddress> bootstrapAddress;
    /**
     * The name of the client used for identification purposes.
     */
    protected String name;
    /**
     * Manages the group of event loop threads responsible for handling I/O operations of the client.
     * This variable is instantiated and used internally by the Client class to process network events,
     * callbacks, and other asynchronous tasks. It is a central component in the framework's event-driven
     * architecture, ensuring efficient handling and dispatching of network messages.
     */
    protected EventLoopGroup workerGroup;
    /**
     * An {@link EventExecutor} responsible for handling tasks related to
     * refreshing metadata for the client. This executor ensures that
     * metadata refresh operations are performed asynchronously to avoid
     * blocking the main thread and improve the responsiveness of the client.
     */
    protected EventExecutor refreshMetadataExecutor;
    /**
     * The Bootstrap instance used by the Client class to initialize and configure
     * the network settings, channels, and other resources necessary for creating
     * and maintaining client connections.
     */
    private Bootstrap bootstrap;
    /**
     * Indicates the running state of the {@code Client}.
     * <p>
     * This variable is used to track whether the {@code Client} is currently running or not.
     * The volatile keyword ensures that the state change is visible to other threads immediately.
     */
    private volatile boolean state = false;

    /**
     * Constructs a new Client instance.
     *
     * @param name the name of the client
     * @param config the configuration for the client, must not be null
     * @param listener the listener to handle various client events, must not be null
     */
    public Client(String name, ClientConfig config, CombineListener listener) {
        this.name = name;
        this.config = Objects.requireNonNull(config, "Client config not found");
        this.listener = Objects.requireNonNull(listener, "Client lister not found");
        this.bootstrapAddress = NetworkUtil.switchSocketAddress(config.getBootstrapAddresses());
    }

    /**
     * Retrieves a bootstrap address from the bootstrapAddress list.
     * If the list is empty, returns null.
     * If the list contains one element, returns that element.
     * If the list contains more than one element, returns a randomly selected element from the list.
     *
     * @return A SocketAddress from the bootstrapAddress list or null if the list is empty.
     */
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

    /**
     * Retrieves the active client channel for the given socket address.
     *
     * @param address the socket address for which the active channel needs to be retrieved. Must not be null.
     * @return the active client channel associated with the specified socket address.
     * @throws MeteorClientException if the address is null or if there is a failure in fetching the client channel.
     */
    public ClientChannel getActiveChannel(SocketAddress address) {
        try {
            return applyChannel(address).get(config.getChannelConnectionTimeoutMilliseconds(), TimeUnit.MILLISECONDS);
        } catch (Throwable t) {
            if (address == null) {
                throw new MeteorClientException(String.format("Client[%s] fetch random channel failed, socket address cannot be empty", name), t);
            }
            throw new MeteorClientException(
                    String.format("Client[%s] fetch random client channel failed and the address is %s", name, address), t);
        }
    }

    /**
     * Retrieves the map of futures for MessageRouter instances keyed by topic.
     *
     * @return ConcurrentMap where the key is a topic as a string and the value is a future that resolves to a MessageRouter instance.
     */
    public ConcurrentMap<String, Future<MessageRouter>> getRouters() {
        return routers;
    }

    /**
     * Attempts to acquire a ClientChannel associated with the given address. If no address is provided, a random
     * ClientChannel is acquired, or a new one is created if necessary. The method ensures that the number of
     * ClientChannels does not exceed the configured connection pool capacity.
     *
     * @param address the SocketAddress to which the ClientChannel is to be associated. Can be null.
     * @return a Future<ClientChannel> that represents the outcome of the operation to acquire or create a ClientChannel.
     *         The Future will complete successfully if a ClientChannel is acquired or created, otherwise it will complete exceptionally.
     * @throws IllegalArgumentException if no bootstrap address is found when no address is provided.
     */
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
                throw new IllegalArgumentException(String.format("Client[%s] Bootstrap address not found", name));
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

    /**
     * Removes a Future<ClientChannel> associated with a specific SocketAddress from the registerChannels map.
     *
     * @param address the SocketAddress key for which the associated Future<ClientChannel> should be removed
     * @param future the Future<ClientChannel> instance to be removed from the list associated with the given address
     */
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

    /**
     * Establishes a connection to the specified remote address and returns a future representing the connected channel.
     *
     * @param address the remote socket address to connect to
     * @return a Future representing the outcome of the connection attempt. If the connection is successful, the Future will complete with a ClientChannel. If the connection fails
     * , the Future will be completed exceptionally with the cause of the failure.
     */
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
            assemblePromise.tryFailure(new IllegalArgumentException(String.format("Client[%s] channel[%s] was closed", name, channel)));
            ChannelOfPromise.remove(channel.id().asLongText());
        });

        return assemblePromise;
    }

    /**
     * Filters the registered channels for the given address and returns a list of ready channels.
     *
     * @param address the SocketAddress to filter channels for
     * @return a list of Futures representing ready ClientChannels, or null if no channels are registered for the address
     */
    private List<Future<ClientChannel>> filter(SocketAddress address) {
        List<Future<ClientChannel>> futures = registerChannels.get(address);
        return futures == null ? null : futures.stream().filter(this::isReady).collect(Collectors.toList());
    }

    /**
     * Checks if a given `Future<ClientChannel>` is ready for use.
     *
     * @param future the future object representing the `ClientChannel`
     * @return true if the future is not null, is not yet done, is successful, and the channel is active; false otherwise
     */
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

    /**
     * Attempts to acquire a random, ready {@link ClientChannel} from the registered channels.
     * The method filters out channels that are not ready and selects a valid channel
     * either by returning the only available one or randomly picking one from the list of valid channels.
     * If no channels are available or all channels are not ready, it returns null.
     *
     * @return a {@link Future} representing a {@link ClientChannel} that is ready, or null if no valid channels are available
     */
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

    /**
     * Checks if the client is currently running.
     *
     * @return true if the client is running, false otherwise.
     */
    public boolean isRunning() {
        return state;
    }

    /**
     * Starts the client by initializing and configuring the necessary resources.
     * <p>
     * This method performs the following operations:
     * 1. Checks if the client is already running and logs a warning if so.
     * 2. Sets the client's state to running.
     * 3. Initializes the worker group with event loop threads.
     * 4. Configures the DNS name resolver with appropriate settings.
     * 5. Sets up the client bootstrap with channel options and resolver.
     * 6. Applies channel settings for bootstrap addresses.
     * 7. Schedules a periodic metadata refresh task.
     * <p>
     * This method is synchronized to ensure thread safety.
     */
    public synchronized void start() {
        if (isRunning()) {
            if (logger.isWarnEnabled()) {
                logger.warn("Client[{}] is running, don't run it replay", name);
            }
            return;
        }

        state = true;
        workerGroup = newEventLoopGroup(config.isSocketEpollPrefer(), config.getWorkerThreadLimit(),
                String.format("client-worker(%s)", name), false, config.isPreferIoUring());
        DnsNameResolverBuilder builder = new DnsNameResolverBuilder();
        builder.ttl(30, 300);
        builder.negativeTtl(30);

        if (config.isSocketEpollPrefer() && Epoll.isAvailable()) {
            builder.datagramChannelType(EpollDatagramChannel.class);
        } else {
            builder.datagramChannelType(NioDatagramChannel.class);
        }

        builder.nameServerProvider(DefaultDnsServerAddressStreamProvider.INSTANCE);
        bootstrap = new Bootstrap()
                .group(workerGroup)
                .channel(NetworkUtil.preferIoUringChannelClass(config.isSocketEpollPrefer(), config.isPreferIoUring()))
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, false)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getChannelConnectionTimeoutMilliseconds())
                .option(ChannelOption.SO_SNDBUF, config.getSocketSendBufferSize())
                .option(ChannelOption.SO_RCVBUF, config.getSocketReceiveBufferSize())
                .resolver(new RoundRobinDnsAddressResolverGroup(builder));
        for (SocketAddress address : bootstrapAddress) {
            applyChannel(address);
        }

        refreshMetadataExecutor = new DefaultEventExecutor(new DefaultThreadFactory(String.format("client(%s)-task", name)));
        refreshMetadataExecutor.schedule(new RefreshMetadataTask(this, config), config.getMetadataRefreshPeriodMilliseconds(), TimeUnit.MILLISECONDS);
    }

    /**
     * Closes the client connection and frees up resources.
     * <p>
     * This method performs a series of steps to gracefully shut down the client:
     * 1. Checks if the client is already running or not. If not running, logs a warning.
     * 2. Sets the client's state to inactive.
     * 3. Shuts down the metadata refreshing executor service gracefully.
     * 4. Waits for the metadata refreshing executor service and worker group to terminate.
     * <p>
     * If an interruption occurs during the waiting process, it sets the interrupt status of the thread.
     */
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

    /**
     * Binds the current client to the provided MeterRegistry to monitor various executors.
     *
     * @param meterRegistry the MeterRegistry to bind to, must not be null
     */
    @Override
    public void bindTo(@Nonnull MeterRegistry meterRegistry) {
        SingleThreadEventExecutor refreshExecutor = (SingleThreadEventExecutor) refreshMetadataExecutor;
        registerMetrics("client-refresh-task", meterRegistry, refreshExecutor);

        for (EventExecutor eventExecutor : workerGroup) {
            SingleThreadEventExecutor workerExecutor = (SingleThreadEventExecutor) eventExecutor;
            registerMetrics("client-worker", meterRegistry, workerExecutor);
        }
    }

    /**
     * Registers metrics for monitoring the specified executor's pending tasks.
     *
     * @param type A string representing the type of the executor.
     * @param meterRegistry The registry to which metrics will be registered.
     * @param executor The executor whose pending tasks will be monitored.
     */
    private void registerMetrics(String type, MeterRegistry meterRegistry, SingleThreadEventExecutor executor) {
        Gauge.builder(CLIENT_NETTY_PENDING_TASK_NAME, executor, SingleThreadEventExecutor::pendingTasks)
                .tag("type", type)
                .tag("name", name)
                .tag("id", executor.threadProperties().name())
                .register(meterRegistry);
    }

    /**
     * Fetches a MessageRouter for the specified topic by utilizing an active client channel.
     *
     * @param topic the topic for which the message router needs to be fetched
     * @return the MessageRouter associated with the specified topic
     * @throws RuntimeException if fetching the message router fails due to any error
     */
    public MessageRouter fetchRouter(String topic) {
        ClientChannel channel = null;
        try {
            channel = getActiveChannel(null);
            return applyRouter(topic, channel, true).get();
        } catch (Throwable t) {
            throw new RuntimeException(String.format("Client[%s] fetch topic[%s] message router from given client channel[%s] failed", name, topic,
                    channel == null ? "NULL" : channel.channel().remoteAddress()), t);
        }
    }

    /**
     * Refreshes the router information for a specific topic on the given client channel.
     *
     * @param topic   The topic for which the router information needs to be refreshed.
     * @param channel The client channel through which the router information is to be refreshed.
     *                If null, an active channel will be retrieved.
     * @throws MeteorClientException if refreshing the router information fails.
     */
    public void refreshRouter(String topic, ClientChannel channel) {
        try {
            if (channel == null) {
                channel = getActiveChannel(null);
            }
            applyRouter(topic, channel, false).get();
        } catch (Throwable t) {
            throw new MeteorClientException(String.format("Client[%s] fetch topic[%s] message router from given client channel[%s] failed", name, topic,
                    channel == null ? "NULL" : channel.channel().remoteAddress()), t);
        }
    }

    /**
     * Checks if the topic has an associated message router.
     *
     * @param topic the topic to be checked for an associated message router
     * @return true if the topic has an associated message router, false otherwise
     */
    public boolean containsRouter(String topic) {
        return routers.containsKey(topic);
    }

    /**
     * Applies a message router to a given topic and channel, optionally using a cached router if available.
     *
     * @param topic The topic for which the router is to be applied.
     * @param channel The client channel through which communication occurs.
     * @param useCached Specifies whether to use a cached router if one exists.
     * @return A Future holding the MessageRouter instance for the given topic.
     */
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

    /**
     * Queries the router for a given topic associated with a specific client channel.
     * Validates the channel's activity status and retrieves cluster and topic information to build the router.
     *
     * @param channel the client channel to query
     * @param topic the topic for which the router is being queried
     * @return a MessageRouter instance for the specified topic if available, or null if the topic information is missing
     * @throws Exception if the channel is inactive or if cluster information cannot be retrieved
     */
    private MessageRouter queryRouter(ClientChannel channel, String topic) throws Exception {
        if (!channel.isActive()) {
            throw new IllegalStateException(String.format("Client[%s] is not active", name));
        }

        ClusterInfo clusterInfo = queryClusterInfo(channel);
        if (clusterInfo == null) {
            throw new IllegalStateException(String.format("Client[%s] that Cluster info not found", name));
        }

        TopicInfo topicInfo = queryTopicInfos(channel, topic).get(topic);
        if (topicInfo == null) {
            return null;
        }
        return buildRouter(topic, clusterInfo, topicInfo);
    }

    /**
     * Builds a MessageRouter for a given topic based on the provided cluster and topic information.
     *
     * @param topic The topic for which the MessageRouter is to be built.
     * @param clusterInfo Information about the cluster, including node metadata.
     * @param topicInfo Information about the topic, including partition metadata.
     * @return A MessageRouter instance configured for the specified topic, or null if no topic metadata is found.
     */
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

    /**
     * Combines two MessageRouter instances by selecting the appropriate MessageLedger instances
     * based on the version and token values.
     *
     * @param cacheRouter the cached MessageRouter instance, may be null
     * @param router the current MessageRouter instance, may be null
     * @return a new MessageRouter instance combining the two input routers, or null if the provided router is null
     */
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

    /**
     * Caches a given {@link MessageRouter} associated with a specific topic,
     * combining it with an existing router in the cache if it exists.
     *
     * @param topic the topic associated with the MessageRouter
     * @param router the MessageRouter to be cached
     * @return the combined MessageRouter if an existing router was found and combined,
     *         otherwise returns the original provided router
     */
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

    /**
     * Queries the cluster information from the specified ClientChannel.
     *
     * @param channel the ClientChannel instance from which to query the cluster information
     * @return the ClusterInfo object containing the cluster information, or null if no cluster information is available
     * @throws Exception if an error occurs during the query
     */
    public ClusterInfo queryClusterInfo(ClientChannel channel) throws Exception {
        QueryClusterInfoRequest request = QueryClusterInfoRequest.newBuilder().build();
        Promise<QueryClusterResponse> promise = ImmediateEventExecutor.INSTANCE.newPromise();
        channel.invoker().queryClusterInfo(config.getMetadataTimeoutMilliseconds(), promise, request);

        QueryClusterResponse response = promise.get(config.getMetadataTimeoutMilliseconds(), TimeUnit.MILLISECONDS);
        return response.hasClusterInfo() ? response.getClusterInfo() : null;
    }

    /**
     * Queries topic information for the specified topics in the context of the given client channel.
     *
     * @param channel the client channel to use for the query
     * @param topics the names of the topics to query
     * @return a map containing topic names as keys and their corresponding {@link TopicInfo} as values
     * @throws Exception if an error occurs during the query
     */
    public Map<String, TopicInfo> queryTopicInfos(ClientChannel channel, String... topics) throws Exception {
        QueryTopicInfoRequest request = QueryTopicInfoRequest.newBuilder()
                .addAllTopicNames(Arrays.asList(topics))
                .build();

        Promise<QueryTopicInfoResponse> promise = ImmediateEventExecutor.INSTANCE.newPromise();
        channel.invoker().queryTopicInfo(config.getMetadataTimeoutMilliseconds(), promise, request);

        return promise.get(config.getMetadataTimeoutMilliseconds(), TimeUnit.MILLISECONDS).getTopicInfosMap();
    }

    /**
     * Creates a new topic with the specified number of partitions and replicas.
     *
     * @param topic the name of the topic to create
     * @param partitions the number of partitions for the topic
     * @param replicas the number of replicas for the topic
     * @return a response object containing details of the created topic
     * @throws Exception if an error occurs while creating the topic
     */
    public CreateTopicResponse createTopic(String topic, int partitions, int replicas) throws Exception {
        return createTopic(topic, partitions, replicas, null);
    }

    /**
     * Creates a new topic with the specified configuration.
     *
     * @param topic The name of the topic to be created.
     * @param partitions The number of partitions for the topic.
     * @param replicas The number of replica nodes for the topic.
     * @param topicConfig The configuration settings for the topic. This parameter is optional.
     * @return A CreateTopicResponse object containing the details of the created topic.
     * @throws Exception If an error occurs during the creation of the topic.
     */
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

    /**
     * Deletes the specified topic from the system.
     *
     * @param topic the name of the topic to be deleted
     * @return a {@link DeleteTopicResponse} containing the result of the delete operation
     * @throws Exception if there is an issue with the delete operation
     */
    public DeleteTopicResponse deleteTopic(String topic) throws Exception {
        TopicPatternUtil.validateTopic(topic);

        DeleteTopicRequest request = DeleteTopicRequest.newBuilder().setTopic(topic).build();
        Promise<DeleteTopicResponse> promise = ImmediateEventExecutor.INSTANCE.newPromise();
        ClientChannel channel = getActiveChannel(null);
        channel.invoker().deleteTopic(config.getDeleteTopicTimeoutMilliseconds(), promise, request);
        return promise.get(config.getDeleteTopicTimeoutMilliseconds(), TimeUnit.MILLISECONDS);
    }

    /**
     * Calculates the partition metadata for the client.
     *
     * @return a response containing the calculated partition information.
     * @throws Exception if an error occurs while calculating the partitions.
     */
    public CalculatePartitionsResponse calculatePartitions() throws Exception {
        ClientChannel clientChannel = getActiveChannel(null);
        Promise<CalculatePartitionsResponse> promise = ImmediateEventExecutor.INSTANCE.newPromise();
        CalculatePartitionsRequest request = CalculatePartitionsRequest.newBuilder().build();
        clientChannel.invoker().calculatePartitions(config.getCalculatePartitionsTimeoutMilliseconds(), promise, request);
        return promise.get(config.getCalculatePartitionsTimeoutMilliseconds(), TimeUnit.MILLISECONDS);
    }

    /**
     * Migrates a ledger from an original broker to a destination broker for a specified topic and partition.
     *
     * @param topic        The topic which contains the ledger to be migrated.
     * @param partition    The specific partition within the topic to be migrated.
     * @param original     The broker currently holding the ledger.
     * @param destination  The broker to which the ledger needs to be migrated.
     * @return A response object containing the result of the migration process.
     * @throws Exception If any error occurs during the migration process.
     */
    public MigrateLedgerResponse migrateLedger(String topic, int partition, String original, String destination) throws Exception {
        MigrateLedgerResponse.Builder response = MigrateLedgerResponse.newBuilder();
        ClientChannel clientChannel = getActiveChannel(null);
        ClusterInfo clusterInfo = queryClusterInfo(clientChannel);
        Map<String, NodeMetadata> nodesMap = clusterInfo.getNodesMap();
        NodeMetadata originalBroker = nodesMap.get(original);
        if (originalBroker == null) {
            return response.setSuccess(false)
                    .setMessage(String.format("The client[%s] that original broker[%s] is not in cluster", name, original)).build();
        }

        if (!nodesMap.containsKey(destination)) {
            return response.setSuccess(false)
                    .setMessage(String.format("The client[%s] that original broker[%s] is not in cluster", name, original))
                    .build();
        }

        Map<String, TopicInfo> topicInfos = queryTopicInfos(clientChannel, topic);
        if (topicInfos == null || topicInfos.isEmpty()) {
            return response.setSuccess(false).setMessage(String.format("The client[%s] of topic[%s] does not exist", name, topic)).build();
        }
        TopicInfo topicInfo = topicInfos.get(topic);
        PartitionMetadata partitionMetadata = topicInfo.getPartitionsMap().get(partition);
        if (partitionMetadata == null) {
            return response.setSuccess(false)
                    .setMessage(String.format("The client[%s] of topic[%s] and partition[%d] are not exist",  name, topic, partition)).build();
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
