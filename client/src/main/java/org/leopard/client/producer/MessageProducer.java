package org.leopard.client.producer;

import static org.leopard.remote.util.NetworkUtils.newImmediatePromise;
import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.leopard.client.Extras;
import org.leopard.client.Message;
import org.leopard.client.internal.Client;
import org.leopard.client.internal.ClientChannel;
import org.leopard.client.internal.metadata.MessageRouter;
import org.leopard.client.internal.metadata.MessageRoutingHolder;
import org.leopard.client.internal.metadata.MetadataWriter;
import org.leopard.client.internal.pool.DefaultFixedChannelPoolFactory;
import org.leopard.client.internal.pool.ShallowChannelPool;
import org.leopard.common.enums.State;
import org.leopard.common.logging.InternalLogger;
import org.leopard.common.logging.InternalLoggerFactory;
import org.leopard.common.util.ObjectUtils;
import org.leopard.remote.proto.server.SendMessageExtras;
import org.leopard.remote.proto.server.SendMessageRequest;
import org.leopard.remote.proto.server.SendMessageResponse;
import org.leopard.remote.util.ByteBufUtils;

public class MessageProducer implements Producer {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MessageProducer.class);

    private MetadataWriter metadataWriter;
    private final ProducerConfig config;
    private ShallowChannelPool pool;
    private final String name;
    private final Client client;
    private final AtomicReference<State> state = new AtomicReference<>(State.LATENT);
    private final Map<Integer, ClientChannel> activeChannels = new Int2ObjectOpenHashMap<>();

    public MessageProducer(String name, ProducerConfig config) {
        this.name = ObjectUtils.checkNonEmpty(name, "Message producer name cannot be null");
        this.client = new Client("producer-client", config.getClientConfig());
        this.config = config;
    }

    @Override
    public void start() throws Exception {
        if (!state.compareAndSet(State.LATENT, State.STARTED)) {
            throw new UnsupportedOperationException("The message producer<" + name + "> was started");
        }

        client.start();

        this.pool = DefaultFixedChannelPoolFactory.INSTANCE.accessChannelPool();
        this.metadataWriter = client.getMetadataWriter();
    }

    @Override
    public void sendOneway(Message message) {
        sendOneway(message, null);
    }

    @Override
    public SendResult send(Message message) throws Exception {
        return send(message, null);
    }

    @Override
    public void sendAsync(Message message, SendCallback callback) {
        sendAsync(message, null, callback);
    }

    @Override
    public void sendOneway(Message message, MessagePreInterceptor messageFilter) {
        checkTopic(message.topic());
        checkQueue(message.queue());

        message = exchange(message, messageFilter);

        doSend(config.getSendOnewayTimeoutMs(), message, null);
    }

    @Override
    public SendResult send(Message message, MessagePreInterceptor messageFilter) throws Exception {
        checkTopic(message.topic());
        checkQueue(message.queue());

        Promise<SendMessageResponse> promise = newImmediatePromise();

        message = exchange(message, messageFilter);

        doSend(config.getSendTimeoutMs(), message, promise);

        SendMessageResponse response =
                promise.get(config.getClientConfig().getInvokeExpiredMs(), TimeUnit.MILLISECONDS);
        return new SendResult(response.getEpoch(), response.getIndex(), response.getLedger());
    }

    @Override
    public void sendAsync(Message message, MessagePreInterceptor messageFilter, SendCallback callback) {
        checkTopic(message.topic());
        checkQueue(message.queue());

        message = exchange(message, messageFilter);

        if (null == callback) {
            doSend(config.getSendAsyncTimeoutMs(), message, null);
            return;
        }

        Promise<SendMessageResponse> promise = newImmediatePromise();
        promise.addListener((GenericFutureListener<Future<SendMessageResponse>>) future -> {
            if (future.isSuccess()) {
                SendMessageResponse response = future.get();
                callback.onCompleted(new SendResult(response.getEpoch(), response.getIndex(), response.getLedger()),
                        null);
            } else {
                callback.onCompleted(null, future.cause());
            }
        });

        doSend(config.getSendAsyncTimeoutMs(), message, promise);
    }

    public void doSend(int timeout, Message message, Promise<SendMessageResponse> promise) {
        String topic = message.topic();
        String queue = message.queue();
        short version = message.version();

        MessageRouter messageRouter = metadataWriter.queryRouter(topic);
        if (null == messageRouter) {
            throw new RuntimeException(String.format("Message router is null, and topic=%s name=%s", topic, name));
        }

        MessageRoutingHolder holder = messageRouter.allocRouteHolder(queue);
        int ledger = holder.ledger();
        SocketAddress leader = holder.leader();
        if (null == leader) {
            throw new IllegalArgumentException(String.format("Leader not found, and ledger=%d name=%s", ledger, name));
        }

        SendMessageRequest request = SendMessageRequest
                .newBuilder()
                .setLedger(ledger)
                .setQueue(queue)
                .build();

        SendMessageExtras extras = buildExtras(topic, queue, message.extras());
        try {
            ByteBuf body = ByteBufUtils.byte2Buf(message.message());

            ClientChannel clientChannel = fetchHealthyChannel(ledger, leader);
            clientChannel.invoker()
                    .invokeMessage(version, timeout, promise, request, extras, body, SendMessageResponse.class);
        } catch (Throwable t) {
            throw new RuntimeException(
                    String.format("Failed to send async message, topic=%s, queue=%s name=%s", topic, queue, name));
        }
    }

    private ClientChannel fetchHealthyChannel(int ledger, SocketAddress address) {
        ClientChannel clientChannel = activeChannels.get(ledger);
        if (clientChannel != null && clientChannel.channel().isActive()) {
            return clientChannel;
        }
        activeChannels.remove(ledger, clientChannel);

        synchronized (activeChannels) {
            ClientChannel channel = activeChannels.get(ledger);
            if (channel != null && channel.channel().isActive()) {
                return channel;
            }

            ClientChannel newChannel = pool.acquireHealthyOrNew(address);
            activeChannels.put(ledger, newChannel);

            return newChannel;
        }
    }

    private SendMessageExtras buildExtras(String topic, String queue, Extras extras) {
        SendMessageExtras.Builder metadata = SendMessageExtras.newBuilder().setQueue(queue).setTopic(topic);
        if (null != extras) {
            for (Map.Entry<String, String> entry : extras) {
                String key = entry.getKey();
                String value = entry.getValue();

                if (null != key && null != value) {
                    metadata.putExtras(key, value);
                }
            }
        }
        return metadata.build();
    }

    private Message exchange(Message message, MessagePreInterceptor mPreInterceptor) {
        if (null != mPreInterceptor) {
            message = mPreInterceptor.interceptor(message);
        }
        return message;
    }

    private void checkTopic(String topic) {
        if (null == topic || topic.isEmpty()) {
            throw new IllegalArgumentException("Send topic cannot be empty");
        }
    }

    private void checkQueue(String queue) {
        if (null == queue || queue.isEmpty()) {
            throw new IllegalArgumentException("Send queue cannot be empty");
        }
    }

    @Override
    public void shutdownGracefully() throws Exception {
        synchronized (this) {
            if (state.compareAndSet(State.STARTED, State.CLOSED)) {
                client.shutdownGracefully();
            }
        }
    }
}
