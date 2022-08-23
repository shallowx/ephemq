package org.shallow.consumer;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import org.shallow.Client;
import org.shallow.ClientConfig;
import org.shallow.invoke.ClientChannel;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.metadata.MessageRouter;
import org.shallow.metadata.MessageRoutingHolder;
import org.shallow.metadata.MetadataManager;
import org.shallow.pool.DefaultFixedChannelPoolFactory;
import org.shallow.pool.ShallowChannelPool;
import org.shallow.proto.server.SubscribeRequest;
import org.shallow.proto.server.SubscribeResponse;
import org.shallow.util.NetworkUtil;

import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

import static org.shallow.processor.ProcessCommand.Server.SUBSCRIBE;
import static org.shallow.util.NetworkUtil.newImmediatePromise;
import static org.shallow.util.ObjectUtil.isNull;

public class MessageConsumer {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MessageConsumer.class);

    private final MetadataManager manager;
    private final ConsumerConfig config;
    private final MessageListener messageListener;
    private final ShallowChannelPool pool;

    public MessageConsumer(Client client, ConsumerConfig config, MessageListener listener) {
        this.messageListener = listener;
        this.config = config;
        this.manager = client.getMetadataManager();
        this.pool = DefaultFixedChannelPoolFactory.INSTANCE.acquireChannelPool();
    }

    public Subscription subscribe(String topic, String queue) {
        checkTopic(topic);
        checkQueue(queue);

        topic = topic.intern();

        Promise<SubscribeResponse> promise = newImmediatePromise();
        try {
            doSubscribe(topic, queue, promise);
            SubscribeResponse response = promise.get(config.getInvokeExpiredMs(), TimeUnit.MILLISECONDS);
            return new Subscription(response.getEpoch(), response.getIndex(), response.getQueue(), response.getLedger());
        } catch (Throwable t) {
            throw new RuntimeException(String.format("Message subscribe failed - topic=%s queue=%s", topic, queue));
        }
    }

    public void subscribeAsync(String topic, String queue, SubscribeCallback callback) {
        checkTopic(topic);
        checkQueue(queue);

        topic = topic.intern();

        if (isNull(callback)) {
            doSubscribe(topic, queue, null);
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
            doSubscribe(topic, queue, promise);
        } catch (Throwable t) {
            throw new RuntimeException(String.format("Message subscribe failed - topic=%s queue=%s", topic, queue));
        }
    }

    private void doSubscribe(String topic, String queue, Promise<SubscribeResponse> promise) {
        MessageRouter messageRouter = manager.queryRouter(topic);
        if (isNull(messageRouter)) {
            throw new RuntimeException(String.format("Message router is empty, and topic=%s", topic));
        }

        MessageRoutingHolder holder = messageRouter.allocRouteHolder(queue);
        int ledger = holder.ledger();
        SocketAddress leader = holder.leader();
        ClientChannel clientChannel = pool.acquireHealthyOrNew(leader);

        SubscribeRequest request = SubscribeRequest
                .newBuilder()
                .setQueue(queue)
                .setLedger(ledger)
                .setEpoch(-1)
                .setIndex(-1)
                .build();

        clientChannel.invoker().invoke(SUBSCRIBE, config.getInvokeExpiredMs(), promise, request, SubscribeResponse.class);
    }

    private void checkTopic(String topic) {
        if (isNull(topic) || topic.isEmpty()) {
            throw new IllegalArgumentException("Subscribe topic cannot be empty");
        }
    }

    private void checkQueue(String queue) {
        if (isNull(queue) || queue.isEmpty()) {
            throw new IllegalArgumentException("Subscribe queue cannot be empty");
        }
    }
}

