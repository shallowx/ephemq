package org.leopard.client.consumer.pull;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import org.leopard.client.internal.ClientChannel;
import org.leopard.client.metadata.MetadataManager;
import org.leopard.client.Client;
import org.leopard.client.State;
import org.leopard.client.consumer.ConsumerConfig;
import org.leopard.client.consumer.MessagePostInterceptor;
import org.leopard.client.metadata.MessageRouter;
import org.leopard.client.metadata.MessageRoutingHolder;
import org.leopard.common.logging.InternalLogger;
import org.leopard.common.logging.InternalLoggerFactory;
import org.leopard.client.pool.ShallowChannelPool;
import org.leopard.remote.proto.server.PullMessageRequest;
import org.leopard.remote.proto.server.PullMessageResponse;
import org.leopard.remote.processor.ProcessCommand;
import org.leopard.remote.util.NetworkUtil;
import org.leopard.common.util.ObjectUtil;

import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicReference;

public class MessagePullConsumer implements PullConsumer {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MessagePullConsumer.class);

    private MetadataManager manager;
    private final ConsumerConfig config;
    private final String name;
    private ShallowChannelPool pool;
    private final Client client;
    private final AtomicReference<State> state = new AtomicReference<>(State.LATENT);
    private MessagePullListener listener;
    private final PullConsumerListener pullConsumerListener;

    public MessagePullConsumer(ConsumerConfig config, String name) {
        this.pullConsumerListener = new PullConsumerListener();
        this.client = new Client("consumer-client", config.getClientConfig(), pullConsumerListener);

        this.config = config;
        this.name = ObjectUtil.checkNonEmpty(name, "Message pull consumer name cannot be null");
    }

    @Override
    public void start() throws Exception {
        if (!state.compareAndSet(State.LATENT, State.STARTED)) {
            throw new UnsupportedOperationException("The message pull consumer<"+ name +"> was started");
        }

        if (null == listener) {
            throw new IllegalArgumentException("Consume<"+ name +">  register message pull listener cannot be null");
        }

        client.start();

        this.manager = client.getMetadataManager();
        this.pool = client.getChanelPool();
    }

    @Override
    public void registerListener(MessagePullListener listener) {
        if (null == listener) {
            throw new IllegalArgumentException("Consume<"+ name +">  register message pull listener cannot be null");
        }
        this.listener = listener;
        this.pullConsumerListener.registerListener(listener);
    }

    @Override
    public void registerInterceptor(MessagePostInterceptor interceptor) {
        pullConsumerListener.registerInterceptor(interceptor);
    }

    @Override
    public void pull(String topic, String queue, short version, int epoch, long index, int limit, MessagePullListener listener,  Promise<PullMessageResponse> promise) throws Exception {
        if (null != listener) {
            this.registerListener(listener);
        }

        if (null == this.listener) {
            throw new IllegalArgumentException("Consume<"+ name +"> message pull listener cannot be null");
        }

        Promise<PullMessageResponse> responsePromise = NetworkUtil.newImmediatePromise();
        responsePromise.addListener((GenericFutureListener<Future<PullMessageResponse>>) future -> {
            if (null != promise) {
                if (future.isSuccess()) {
                    promise.trySuccess(future.get());
                } else {
                    promise.tryFailure(future.cause());
                }
            }

            if (logger.isDebugEnabled()) {
                logger.debug("Consume<"+ name +"> promise is null, and there will be no callback");
            }
        });
        doPullMessage(topic, queue, version, epoch, index, limit, responsePromise);
    }

    @Override
    public void pull(String topic, String queue, int epoch, long index, int limit, MessagePullListener listener, Promise<PullMessageResponse> promise) throws Exception {
        this.pull(topic, queue, (short)-1, epoch, index, limit, listener, promise);
    }

    @Override
    public void pull(String topic, String queue, int epoch, long index, int limit, MessagePullListener listener) throws Exception {
        this.pull(topic, queue, (short)-1, epoch, index, limit, listener, null);
    }

    @Override
    public void pull(String topic, String queue, int epoch, long index, int limit, Promise<PullMessageResponse> promise) throws Exception {
        this.pull(topic, queue, (short)-1, epoch, index, limit, null, promise);
    }

    @Override
    public void pull(String topic, String queue, int epoch, long index, int limit) throws Exception {
        this.pull(topic, queue, (short)-1, epoch, index, limit, null, null);
    }

    @Override
    public void pull(String topic, String queue, short version, int epoch, long index, int limit, Promise<PullMessageResponse> promise) throws Exception {
        this.pull(topic, queue, version, epoch, index, limit, null,  promise);
    }

    @Override
    public void pull(String topic, String queue, short version, int epoch, long index, int limit) throws Exception {
        this.pull(topic, queue, version, epoch, index, limit, null, null);
    }

    @Override
    public void pull(String topic, String queue, short version, int epoch, long index, int limit, MessagePullListener listener) throws Exception {
        this.pull(topic, queue, version, epoch, index, limit, listener, null);
    }


    @Override
    public MessagePullListener getListener() {
        return listener;
    }

    private void doPullMessage(String topic, String queue, short version, int epoch, long index, int limit, Promise<PullMessageResponse> promise) {
        MessageRouter messageRouter = manager.queryRouter(topic);
        if (null == messageRouter) {
            throw new RuntimeException(String.format("Consume<"+ name +"> the topic<%s> router is null", topic));
        }

        MessageRoutingHolder holder = messageRouter.allocRouteHolder(queue);
        if (null == holder) {
            throw new RuntimeException(String.format("Consume<"+ name +"> the topic<%s> ledgers is null", topic));
        }
        int ledger = holder.ledger();

        PullMessageRequest request = PullMessageRequest
                .newBuilder()
                .setLedger(ledger)
                .setTopic(topic)
                .setEpoch(epoch)
                .setIndex(index)
                .setQueue(queue)
                .setLimit(limit)
                .setVersion(version)
                .build();

        SocketAddress leader = holder.leader();
        ClientChannel clientChannel = pool.acquireHealthyOrNew(leader);
        clientChannel.invoker().invoke(ProcessCommand.Server.PULL_MESSAGE, config.getPullInvokeTimeMs(), promise, request, PullMessageResponse.class);
    }

    @Override
    public void shutdownGracefully() throws Exception {
        if (state.compareAndSet(State.STARTED, State.CLOSED)) {
            client.shutdownGracefully();
        }
    }
}
