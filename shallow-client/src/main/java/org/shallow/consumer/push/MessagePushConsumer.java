package org.shallow.consumer.push;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import org.shallow.Client;
import org.shallow.State;
import org.shallow.consumer.ConsumerConfig;
import org.shallow.consumer.MessagePostFilter;
import org.shallow.internal.ClientChannel;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.metadata.MessageRouter;
import org.shallow.metadata.MessageRoutingHolder;
import org.shallow.metadata.MetadataManager;
import org.shallow.pool.ShallowChannelPool;
import org.shallow.proto.server.CleanSubscribeRequest;
import org.shallow.proto.server.CleanSubscribeResponse;
import org.shallow.proto.server.SubscribeRequest;
import org.shallow.proto.server.SubscribeResponse;
import org.shallow.util.NetworkUtil;
import org.shallow.util.ObjectUtil;

import java.net.SocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.shallow.processor.ProcessCommand.Server.CLEAN_SUBSCRIBE;
import static org.shallow.processor.ProcessCommand.Server.SUBSCRIBE;
import static org.shallow.util.NetworkUtil.newEventExecutorGroup;
import static org.shallow.util.NetworkUtil.newImmediatePromise;

@SuppressWarnings("all")
public class MessagePushConsumer implements PushConsumer {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MessagePushConsumer.class);

    private MetadataManager manager;
    private final ConsumerConfig config;
    private MessagePushListener messageListener;
    private ShallowChannelPool pool;
    private final String name;
    private final Client client;
    private final PushConsumerListener pushConsumerListener;
    private final AtomicReference<State> state = new AtomicReference<>(State.LATENT);
    private final Map<String, String> subscribes = new ConcurrentHashMap<>();
    private final Map<Integer, ClientChannel> ledgerOfChannels = new ConcurrentHashMap<>();
    private final EventExecutor subscribeTaskExecutor;

    public MessagePushConsumer(String name, ConsumerConfig config) {
        this.pushConsumerListener = new PushConsumerListener(config, this);
        this.client = new Client("consumer-client", config.getClientConfig(), pushConsumerListener);
        this.config = config;

        this.subscribeTaskExecutor = newEventExecutorGroup(1, name + "-subscribe-task").next();

        this.name = ObjectUtil.checkNonEmpty(name, "Message push consumer name cannot be null");
    }

    @Override
    public void start() throws Exception {
        if (!state.compareAndSet(State.LATENT, State.STARTED)) {
            throw new UnsupportedOperationException("The message pull consumer<"+ name +"> was started");
        }

        if (null == messageListener) {
            throw new IllegalArgumentException("Consume<"+ name +">  register message push listener cannot be null");
        }

        client.start();

        this.manager = client.getMetadataManager();
        this.pool = client.getChanelPool();
    }

    @Override
    public void registerListener(MessagePushListener listener) {
        if (null == listener) {
            throw new IllegalArgumentException("Consume<"+ name +">  register message push listener cannot be null");
        }
        this.messageListener = listener;
        pushConsumerListener.registerListener(listener);
    }

    @Override
    public void registerFilter(MessagePostFilter filter) {
        pushConsumerListener.registerFilter(filter);
    }

    @Override
    public MessagePushListener getListener() {
        return messageListener;
    }

    @Override
    public Subscription subscribe(String topic, String queue, short version, int epoch, long index) {
        return this.subscribe(topic, queue, version, epoch, index, null);
    }

    @Override
    public void subscribeAsync(String topic, String queue, short version, int epoch, long index, SubscribeCallback callback) {
        this.subscribeAsync(topic, queue, (short) -1, epoch, index, callback, null);
    }

    @Override
    public Subscription subscribe(String topic, String queue, short version, int epoch, long index, MessagePushListener listener) {
        checkTopic(topic);
        checkQueue(queue);

        if (null != listener) {
            this.registerListener(listener);
        }

        topic = topic.intern();
        Promise<SubscribeResponse> promise = newImmediatePromise();
        try {
            doSubscribe(topic, queue, version, epoch, index, promise);
            SubscribeResponse response = promise.get(config.getClientConfig().getInvokeExpiredMs(), TimeUnit.MILLISECONDS);
            return new Subscription(response.getEpoch(), response.getIndex(), response.getQueue(), response.getLedger(), (short)response.getVersion());
        } catch (Throwable t) {
            throw new RuntimeException(String.format("Message subscribe failed - topic=%s queue=%s, error:%s", topic, queue, t));
        }
    }

    @Override
    public void subscribeAsync(String topic, String queue, short version, int epoch, long index, SubscribeCallback callback, MessagePushListener listener) {
        checkTopic(topic);
        checkQueue(queue);

        if (null != listener) {
            this.registerListener(listener);
        }

        topic = topic.intern();
        if (null == callback) {
            doSubscribe(topic, queue, version,epoch, index, null);
            return;
        }
        Promise<SubscribeResponse> promise = newImmediatePromise();
        promise.addListener((GenericFutureListener<Future<SubscribeResponse>>) future -> {
            if (future.isSuccess()) {
                SubscribeResponse response = future.get();
                Subscription subscription = new Subscription(response.getEpoch(), response.getIndex(), response.getQueue(), response.getLedger(), (short) response.getVersion());
                callback.onCompleted(subscription, null);
            } else {
                callback.onCompleted(null, future.cause());
            }
        });

        try {
            doSubscribe(topic, queue,version, epoch, index, promise);
        } catch (Throwable t) {
            throw new RuntimeException(String.format("Message subscribe failed - topic=%s queue=%s name=%s error:%s", topic, queue, name, t));
        }
    }

    @Override
    public Subscription subscribe(String topic, String queue, int epoch, long index) {
        return this.subscribe(topic, queue, epoch, index, null);
    }

    @Override
    public void subscribeAsync(String topic, String queue, int epoch, long index, SubscribeCallback callback) {
        this.subscribeAsync(topic, queue, (short) -1, epoch, index, callback, null);
    }

    @Override
    public Subscription subscribe(String topic, String queue, int epoch, long index, MessagePushListener listener) {
        return this.subscribe(topic, queue, (short) -1, epoch, index, listener);
    }

    @Override
    public void subscribeAsync(String topic, String queue, int epoch, long index, SubscribeCallback callback, MessagePushListener listener) {
        this.subscribeAsync(topic, queue, (short) -1, epoch, index, callback, listener);
    }

    @Override
    public Subscription subscribe(String topic, String queue, short version) {
        return this.subscribe(topic, queue, (short) -1, null);
    }

    @Override
    public void subscribeAsync(String topic, String queue, short version, SubscribeCallback callback) {
        this.subscribeAsync(topic, queue, version, callback, null);
    }

    @Override
    public Subscription subscribe(String topic, String queue, short version, MessagePushListener listener) {
        return this.subscribe(topic, queue, version, (short)-1, (short)-1, listener);
    }

    @Override
    public void subscribeAsync(String topic, String queue, short version, SubscribeCallback callback, MessagePushListener listener) {
        this.subscribeAsync(topic, queue, version, (short)-1, (short)-1, callback, listener);
    }

    @Override
    public Subscription subscribe(String topic, String queue) {
        return this.subscribe(topic, queue, (short)-1);
    }

    @Override
    public void subscribeAsync(String topic, String queue, SubscribeCallback callback) {
        this.subscribeAsync(topic, queue, (short)-1, callback, null);
    }

    @Override
    public Subscription subscribe(String topic, String queue, MessagePushListener listener) {
        if (null == listener) {
            throw new IllegalArgumentException("Consume<"+ name +">  register message push listener cannot be null");
        }
        this.registerListener(listener);
        return this.subscribe(topic, queue, (short)-1, listener);
    }

    @Override
    public void subscribeAsync(String topic, String queue, SubscribeCallback callback, MessagePushListener listener) {
        this.subscribeAsync(topic, queue,(short) -1, (short)-1, (short)-1, callback, listener);
    }

    private void doSubscribe(String topic, String queue, short version, int epoch, long index, Promise<SubscribeResponse> promise) {
        MessageRouter messageRouter = manager.queryRouter(topic);
        if (null == messageRouter) {
            throw new RuntimeException(String.format("Message router is empty, and topic=%s name=%s", topic, name));
        }

        MessageRoutingHolder holder = messageRouter.allocRouteHolder(queue);
        int ledger = holder.ledger();
        ClientChannel clientChannel = acquireChannel(holder);

        if (clientChannel == null) {
            throw new RuntimeException(String.format("Any active channel not found for topic<%s> of ledger<%d>", topic, holder.ledger()));
        }

        SubscribeRequest request = SubscribeRequest
                .newBuilder()
                .setQueue(queue)
                .setLedger(ledger)
                .setVersion(version)
                .setEpoch(epoch)
                .setIndex(index)
                .build();

        promise.addListener(f -> {
            if (f.isSuccess()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Subscribe successfully, topic={} queue={} ledger={} version={}, epoch={} index={}", topic, queue, ledger, epoch, index);
                }
                pushConsumerListener.set(epoch, index, queue, ledger, version);
                subscribes.put(topic, queue);
            }
        });

        ledgerOfChannels.put(ledger, clientChannel);
        clientChannel.invoker().invoke(SUBSCRIBE, config.getPushSubscribeInvokeTimeMs(), promise, request, SubscribeResponse.class);
    }

    @Override
    public boolean clean(String topic, String queue) {
        checkTopic(topic);
        checkQueue(queue);

        topic = topic.intern();

        Promise<CleanSubscribeResponse> promise = newImmediatePromise();
        doClean(topic, queue, promise);
        return promise.isSuccess();
    }

    @Override
    public void cleanAsync(String topic, String queue, CleanSubscribeCallback callback) {
        checkTopic(topic);
        checkQueue(queue);

        topic = topic.intern();

        Promise<CleanSubscribeResponse> promise = newImmediatePromise();
        promise.addListener((GenericFutureListener<Future<CleanSubscribeResponse>>) future -> {
            if (future.isSuccess()) {
                callback.onCompleted(null);
            } else {
                callback.onCompleted(future.cause());
            }
        });

        doClean(topic, queue, promise);
    }

    public void resetSuscribe(String node, SocketAddress address) {
        try {
            manager.refreshMetadata();

            Map<String, MessageRouter> wholesRoutes = manager.getWholesRoutes();
            Set<Map.Entry<String, MessageRouter>> entries = wholesRoutes.entrySet();

            for (Map.Entry<String, MessageRouter> entry : entries) {
                MessageRouter router = entry.getValue();
                Map<Integer, MessageRoutingHolder> holders = router.getHolders();

                Set<Map.Entry<Integer, MessageRoutingHolder>> holderEnties = holders.entrySet();
                for (Map.Entry<Integer, MessageRoutingHolder> holderEntry : holderEnties) {
                    MessageRoutingHolder holder = holderEntry.getValue();
                    SocketAddress leader = holder.leader();
                    if (leader.equals(address)) {
                        String topic = holder.topic();

                        int ledger = holder.ledger();
                        AtomicReference<Subscription> subscriptionShip = pushConsumerListener.getSubscriptionShip(ledger);
                        Subscription subscription = subscriptionShip.get();
                        String queue = subscription.queue();
                        short version = subscription.version();
                        int epoch = subscription.epoch();
                        long index = subscription.index();

                        try {
                            subscribeAsync(topic, queue, version, epoch, index, new SubscribeCallback() {
                                @Override
                                public void onCompleted(Subscription subscription, Throwable cause) {
                                    if (cause != null) {
                                        retryTask(topic, queue, version, epoch, index);
                                    }
                                }
                            }, messageListener);
                        } catch (Throwable t) {
                            if (logger.isDebugEnabled()) {
                                logger.debug("Retry subscribe was failure, and try again later, topic={} queue={} version={} epoch={} index={}", topic, queue, version, epoch, index);
                            }
                            retryTask(topic, queue, version, epoch, index);
                        }
                    }
                }
            }
        } catch (Throwable t) {
            if (!subscribeTaskExecutor.isShutdown()) {
                subscribeTaskExecutor.schedule(() -> {
                    try {
                        resetSuscribe(node, address);
                    } catch (Throwable cause) {
                        subscribeTaskExecutor.schedule(() -> resetSuscribe(node, address), 0, TimeUnit.MILLISECONDS);
                    }
                }, 0, TimeUnit.MILLISECONDS);
            }
        }
    }

    private void retryTask(String topic, String queue, short version, int epoch, long index) {
        Promise<Object> promise = newImmediatePromise();
        promise.addListener(f -> {
            if (!f.isSuccess()) {
                retryTask(topic, queue, version, epoch, index);
            }
        });

        try {
            subscribeTaskExecutor.schedule(() -> {
                        try {
                            subscribeAsync(topic, queue, version, epoch, index, new SubscribeCallback() {
                                @Override
                                public void onCompleted(Subscription subscription, Throwable cause) {
                                    if (cause != null) {
                                        promise.tryFailure(cause);
                                    } else {
                                        promise.trySuccess(null);
                                    }
                                }
                            }, messageListener);
                        } catch (Throwable t) {
                            promise.tryFailure(t);
                        }
                    }, 0, TimeUnit.MILLISECONDS);

        } catch (Throwable t) {
            promise.tryFailure(t);
        }
    }

    private ClientChannel acquireChannel(MessageRoutingHolder holder) {
        int ledger = holder.ledger();
        String topic = holder.topic();

        ClientChannel clientChannel = ledgerOfChannels.get(ledger);
        if (clientChannel != null && clientChannel.channel().isActive()) {
            return clientChannel;
        }

        synchronized (ledgerOfChannels) {
            ClientChannel newChannel = ledgerOfChannels.get(ledger);
            if (clientChannel != null && clientChannel.channel().isActive()) {
                return newChannel;
            }
            SocketAddress leader = holder.leader();
            if (leader == null) {
                throw new IllegalArgumentException(String.format("No leader found for topic<%s> of ledger<%d>", topic, ledger));
            }

            ClientChannel newLeaderChannel = pool.acquireHealthyOrNew(leader);
            if (newChannel != null && newChannel.isActive()) {
                return newLeaderChannel;
            }

            Set<SocketAddress> latencies = holder.latencies();
            if (!latencies.isEmpty()) {
                for (SocketAddress latency : latencies) {
                    ClientChannel latencyChannel = pool.acquireHealthyOrNew(latency);
                    if (latencyChannel != null && latencyChannel.isActive()) {
                        return latencyChannel;
                    }
                }
            }

            if (logger.isWarnEnabled()) {
                logger.warn("Channel not found, topic={} ledger={}", topic, ledger);
            }
            return null;
        }
    }

    private void doClean(String topic, String queue, Promise<CleanSubscribeResponse> promise) {
        MessageRouter messageRouter = manager.queryRouter(topic);
        if (null == messageRouter) {
            throw new RuntimeException(String.format("Message router is null, and topic=%s name=%s", topic, name));
        }

        MessageRoutingHolder holder = messageRouter.allocRouteHolder(queue);
        int ledger = holder.ledger();
        SocketAddress leader = holder.leader();
        ClientChannel clientChannel = pool.acquireHealthyOrNew(leader);

        CleanSubscribeRequest request = CleanSubscribeRequest
                .newBuilder()
                .setQueue(queue)
                .setLedgerId(ledger)
                .build();
        clientChannel.invoker().invoke(CLEAN_SUBSCRIBE, config.getPushCleanSubscribeInvokeTimeMs(), promise, request, CleanSubscribeResponse.class);
    }

    private void checkTopic(String topic) {
        if (null == topic || topic.isEmpty()) {
            throw new IllegalArgumentException("Subscribe topic cannot be null");
        }
    }

    private void checkQueue(String queue) {
        if (null == queue || queue.isEmpty()) {
            throw new IllegalArgumentException("Subscribe queue cannot be null");
        }
    }

    @Override
    public void shutdownGracefully() throws Exception {
        if (!subscribeTaskExecutor.isShutdown()) {
            subscribeTaskExecutor.shutdownGracefully();
        }

        if (state.compareAndSet(State.STARTED, State.CLOSED)) {
            client.shutdownGracefully();
        }
    }
}

