package org.shallow.consumer.push;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import org.shallow.Client;
import org.shallow.State;
import org.shallow.consumer.ConsumerConfig;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.shallow.processor.ProcessCommand.Server.CLEAN_SUBSCRIBE;
import static org.shallow.processor.ProcessCommand.Server.SUBSCRIBE;
import static org.shallow.util.NetworkUtil.newImmediatePromise;

public class MessagePushConsumer implements PushConsumer {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MessagePushConsumer.class);

    private MetadataManager manager;
    private final ConsumerConfig config;
    private MessagePushListener messageListener;
    private ShallowChannelPool pool;
    private final String name;
    private final Client client;
    private final AtomicReference<State> state = new AtomicReference<>(State.LATENT);
    private final PushConsumerListener pushConsumerListener;

    public MessagePushConsumer(String name, ConsumerConfig config) {
        this.pushConsumerListener = new PushConsumerListener(config);
        this.client = new Client("consumer-client", config.getClientConfig(), pushConsumerListener);
        this.config = config;

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
            return new Subscription(response.getEpoch(), response.getIndex(), response.getQueue(), response.getLedger());
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
                Subscription subscription = new Subscription(response.getEpoch(), response.getIndex(), response.getQueue(), response.getLedger());
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
        SocketAddress leader = holder.leader();
        ClientChannel clientChannel = pool.acquireHealthyOrNew(leader);

        SubscribeRequest request = SubscribeRequest
                .newBuilder()
                .setQueue(queue)
                .setLedger(ledger)
                .setVersion(version)
                .setEpoch(epoch)
                .setIndex(index)
                .build();

        pushConsumerListener.set(epoch, index, queue, ledger);
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
        if (state.compareAndSet(State.STARTED, State.CLOSED)) {
            client.shutdownGracefully();
        }
    }
}

