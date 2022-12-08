package org.ostara.client.consumer;

import static org.ostara.remote.processor.ProcessCommand.Server.CLEAN_SUBSCRIBE;
import static org.ostara.remote.processor.ProcessCommand.Server.SUBSCRIBE;
import static org.ostara.remote.util.NetworkUtils.newEventExecutorGroup;
import static org.ostara.remote.util.NetworkUtils.newImmediatePromise;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.ostara.client.internal.Client;
import org.ostara.client.internal.ClientChannel;
import org.ostara.client.internal.metadata.MessageRouter;
import org.ostara.client.internal.metadata.MessageRoutingHolder;
import org.ostara.client.internal.metadata.MetadataSupport;
import org.ostara.client.internal.pool.ShallowChannelPool;
import org.ostara.common.enums.State;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.common.metadata.Subscription;
import org.ostara.common.util.ObjectUtils;
import org.ostara.remote.proto.server.CleanSubscribeRequest;
import org.ostara.remote.proto.server.CleanSubscribeResponse;
import org.ostara.remote.proto.server.SubscribeRequest;
import org.ostara.remote.proto.server.SubscribeResponse;

@SuppressWarnings("all")
public class MessageConsumer implements Consumer {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MessageConsumer.class);

    private MetadataSupport manager;
    private final ConsumerConfig config;
    private MessageListener messageListener;
    private ShallowChannelPool pool;
    private final String name;
    private final Client client;
    private final MessageConsumerListener pushConsumerListener;
    private final AtomicReference<State> state = new AtomicReference<>(State.LATENT);
    private final Map<ClientChannel, Map<String, String>> subscribes = new ConcurrentHashMap<>();
    private final Map<Integer, ClientChannel> ledgerOfChannels = new ConcurrentHashMap<>();
    private final EventExecutor subscribeTaskExecutor;

    public MessageConsumer(String name, ConsumerConfig config) {
        this.pushConsumerListener = new MessageConsumerListener(config, this);
        this.client = new Client("consumer-client", config.getClientConfig(), pushConsumerListener);
        this.config = config;

        this.subscribeTaskExecutor = newEventExecutorGroup(1, name + "-subscribe-task").next();

        this.name = ObjectUtils.checkNonEmpty(name, "Message push consumer name cannot be null");
    }

    @Override
    public void start() throws Exception {
        if (!state.compareAndSet(State.LATENT, State.STARTED)) {
            throw new UnsupportedOperationException("The message pull consumer<" + name + "> was started");
        }

        if (null == messageListener) {
            throw new IllegalArgumentException("Consume<" + name + ">  register message push listener cannot be null");
        }

        client.start();

        this.manager = client.getMetadataSupport();
        this.pool = client.getChanelPool();

        subscribeTaskExecutor.schedule(() -> resetSuscribe(null), 5000, TimeUnit.MILLISECONDS);
    }

    @Override
    public void registerListener(MessageListener listener) {
        if (null == listener) {
            throw new IllegalArgumentException("Consume<" + name + ">  register message push listener cannot be null");
        }
        this.messageListener = listener;
        pushConsumerListener.registerListener(listener);
    }

    @Override
    public void registerInterceptor(MessagePostInterceptor interceptor) {
        pushConsumerListener.registerInterceptor(interceptor);
    }

    @Override
    public MessageListener getListener() {
        return messageListener;
    }

    @Override
    public Subscription subscribe(String topic, String queue, short version, int epoch, long index) {
        return this.subscribe(topic, queue, version, epoch, index, null);
    }

    @Override
    public void subscribeAsync(String topic, String queue, short version, int epoch, long index,
                               SubscribeCallback callback) {
        this.subscribeAsync(topic, queue, version, epoch, index, callback, null);
    }

    @Override
    public Subscription subscribe(String topic, String queue, short version, int epoch, long index,
                                  MessageListener listener) {
        checkTopic(topic);
        checkQueue(queue);

        if (null != listener) {
            this.registerListener(listener);
        }

        topic = topic.intern();
        Promise<SubscribeResponse> promise = newImmediatePromise();
        try {
            doSubscribe(topic, queue, version, epoch, index, promise);
            SubscribeResponse response =
                    promise.get(config.getClientConfig().getInvokeExpiredMs(), TimeUnit.MILLISECONDS);
            return Subscription
                    .newBuilder()
                    .queue(response.getQueue())
                    .epoch(response.getEpoch())
                    .ledger(response.getLedger())
                    .version((short) response.getVersion())
                    .index(response.getIndex())
                    .build();
        } catch (Throwable t) {
            throw new RuntimeException(
                    String.format("Message subscribe failed - topic=%s queue=%s, error:%s", topic, queue, t));
        }
    }

    @Override
    public void subscribeAsync(String topic, String queue, short version, int epoch, long index,
                               SubscribeCallback callback, MessageListener listener) {
        checkTopic(topic);
        checkQueue(queue);

        if (null != listener) {
            this.registerListener(listener);
        }

        topic = topic.intern();
        if (null == callback) {
            doSubscribe(topic, queue, version, epoch, index, null);
            return;
        }
        Promise<SubscribeResponse> promise = newImmediatePromise();
        promise.addListener((GenericFutureListener<Future<SubscribeResponse>>) future -> {
            if (future.isSuccess()) {
                SubscribeResponse response = future.get();
                Subscription subscription = Subscription
                        .newBuilder()
                        .version((short) response.getVersion())
                        .ledger(response.getLedger())
                        .epoch(response.getEpoch())
                        .index(response.getIndex())
                        .queue(response.getQueue())
                        .ledger(response.getLedger())
                        .build();
                callback.onCompleted(subscription, null);
            } else {
                callback.onCompleted(null, future.cause());
            }
        });

        try {
            doSubscribe(topic, queue, version, epoch, index, promise);
        } catch (Throwable t) {
            throw new RuntimeException(
                    String.format("Message subscribe failed - topic=%s queue=%s name=%s error:%s", topic, queue, name,
                            t));
        }
    }

    @Override
    public Subscription subscribe(String topic, String queue, int epoch, long index) {
        return this.subscribe(topic, queue, (short) -1, epoch, index, null);
    }

    @Override
    public void subscribeAsync(String topic, String queue, int epoch, long index, SubscribeCallback callback) {
        this.subscribeAsync(topic, queue, (short) -1, epoch, index, callback, null);
    }

    @Override
    public Subscription subscribe(String topic, String queue, int epoch, long index, MessageListener listener) {
        return this.subscribe(topic, queue, (short) -1, epoch, index, listener);
    }

    @Override
    public void subscribeAsync(String topic, String queue, int epoch, long index, SubscribeCallback callback,
                               MessageListener listener) {
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
    public Subscription subscribe(String topic, String queue, short version, MessageListener listener) {
        return this.subscribe(topic, queue, version, (short) -1, (short) -1, listener);
    }

    @Override
    public void subscribeAsync(String topic, String queue, short version, SubscribeCallback callback,
                               MessageListener listener) {
        this.subscribeAsync(topic, queue, version, (short) -1, (short) -1, callback, listener);
    }

    @Override
    public Subscription subscribe(String topic, String queue) {
        return this.subscribe(topic, queue, (short) -1);
    }

    @Override
    public void subscribeAsync(String topic, String queue, SubscribeCallback callback) {
        this.subscribeAsync(topic, queue, (short) -1, callback, null);
    }

    @Override
    public Subscription subscribe(String topic, String queue, MessageListener listener) {
        if (null == listener) {
            throw new IllegalArgumentException("Consume<" + name + ">  register message push listener cannot be null");
        }
        this.registerListener(listener);
        return this.subscribe(topic, queue, (short) -1, listener);
    }

    @Override
    public void subscribeAsync(String topic, String queue, SubscribeCallback callback, MessageListener listener) {
        this.subscribeAsync(topic, queue, (short) -1, (short) -1, (short) -1, callback, listener);
    }

    private void doSubscribe(String topic, String queue, short version, int epoch, long index,
                             Promise<SubscribeResponse> promise) {
        MessageRouter messageRouter = manager.queryRouter(topic);
        if (null == messageRouter) {
            throw new RuntimeException(String.format("Message router is empty, and topic=%s name=%s", topic, name));
        }

        MessageRoutingHolder holder = messageRouter.allocRouteHolder(queue);
        int ledger = holder.ledger();
        ClientChannel clientChannel = acquireChannel(holder);

        if (clientChannel == null) {
            throw new RuntimeException(
                    String.format("Any active channel not found for topic<%s> of ledger<%d>", topic, holder.ledger()));
        }

        SubscribeRequest request = SubscribeRequest.newBuilder()
                .setQueue(queue)
                .setTopic(topic)
                .setLedger(ledger)
                .setVersion(version)
                .setEpoch(epoch)
                .setIndex(index)
                .build();

        promise.addListener(f -> {
            if (f.isSuccess()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Subscribe successfully, topic={} queue={} ledger={} version={}, epoch={} index={}",
                            topic, queue, ledger, epoch, index);
                }
                pushConsumerListener.set(epoch, index, queue, ledger, version);

                Map<String, String> subscribeShips = subscribes.get(clientChannel);
                if (subscribeShips == null || subscribeShips.isEmpty()) {
                    subscribeShips = new ConcurrentHashMap<>();
                }
                subscribeShips.put(topic, queue);
                subscribes.put(clientChannel, subscribeShips);
            }
        });

        ledgerOfChannels.put(ledger, clientChannel);
        clientChannel.invoker()
                .invoke(SUBSCRIBE, config.getSubscribeInvokeTimeMs(), promise, request, SubscribeResponse.class);
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

    public void resetSuscribe(SocketAddress address) {
        try {
            if (subscribes.isEmpty()) {
                return;
            }

            Set<Map.Entry<ClientChannel, Map<String, String>>> entries = subscribes.entrySet();
            for (Map.Entry<ClientChannel, Map<String, String>> entry : entries) {
                ClientChannel clientChannel = entry.getKey();

                SocketAddress subscribeAddress = clientChannel.address();
                if (address != null && !subscribeAddress.equals(address)) {
                    continue;
                }

                if (clientChannel.isActive()) {
                    continue;
                }

                Map<String, String> topicQueues = entry.getValue();
                if (topicQueues.isEmpty()) {
                    continue;
                }

                Set<Map.Entry<String, String>> topicQueueEntries = topicQueues.entrySet();
                for (Map.Entry<String, String> topicQueueEntry : topicQueueEntries) {
                    String topic = topicQueueEntry.getKey();
                    String queue = topicQueueEntry.getValue();

                    MessageRouter messageRouter = manager.queryRouter(topic);
                    if (null == messageRouter) {
                        throw new RuntimeException(
                                String.format("Message rest subscribe router is empty, and topic=%s name=%s", topic,
                                        name));
                    }

                    MessageRoutingHolder holder = messageRouter.allocRouteHolder(queue);
                    int ledger = holder.ledger();

                    AtomicReference<Subscription> subscriptionShip = pushConsumerListener.getSubscriptionShip(ledger);
                    Subscription subscription = subscriptionShip.get();

                    short version = subscription.getVersion();
                    int epoch = subscription.getEpoch();
                    long index = subscription.getIndex();

                    subscribeAsync(topic, queue, version, index, null, messageListener);
                }
            }

        } catch (Throwable t) {
            if (!subscribeTaskExecutor.isShutdown()) {
                subscribeTaskExecutor.schedule(() -> {
                    try {
                        resetSuscribe(address);
                    } catch (Throwable cause) {
                        if (subscribeTaskExecutor.isShutdown()) {
                            return;
                        }
                        subscribeTaskExecutor.schedule(() -> resetSuscribe(address), 1000, TimeUnit.MILLISECONDS);
                    }
                }, 1000, TimeUnit.MILLISECONDS);
            }
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
                throw new IllegalArgumentException(
                        String.format("No leader found for topic<%s> of ledger<%d>", topic, ledger));
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
                .setTopic(topic)
                .setLedgerId(ledger)
                .build();

        promise.addListener(f -> {
            Collection<Map<String, String>> subscribeModel = subscribes.values();
            if (subscribeModel == null || subscribeModel.isEmpty()) {
                return;
            }

            for (Map<String, String> map : subscribeModel) {
                Set<Map.Entry<String, String>> entries = map.entrySet();
                for (Map.Entry<String, String> entry : entries) {
                    if (entry.getKey().equals(topic) && entry.getValue().equals(queue)) {
                        map.remove(entry.getKey(), entry.getValue());
                    }
                }
            }
        });

        clientChannel.invoker().invoke(CLEAN_SUBSCRIBE, config.getCleanSubscribeInvokeTimeMs(), promise, request,
                CleanSubscribeResponse.class);
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

