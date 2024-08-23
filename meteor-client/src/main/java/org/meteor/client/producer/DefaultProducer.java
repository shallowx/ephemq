package org.meteor.client.producer;

import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.meteor.client.core.Client;
import org.meteor.client.core.ClientChannel;
import org.meteor.client.core.CombineListener;
import org.meteor.client.core.MessageLedger;
import org.meteor.client.core.MessageRouter;
import org.meteor.client.exception.RemotingSendRequestException;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.common.message.MessageId;
import org.meteor.common.util.TopicPatternUtil;
import org.meteor.remote.proto.MessageMetadata;
import org.meteor.remote.proto.server.SendMessageRequest;
import org.meteor.remote.proto.server.SendMessageResponse;
import org.meteor.remote.util.ByteBufUtil;

public class DefaultProducer implements Producer {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(DefaultProducer.class);
    private final String name;
    private final ProducerConfig config;
    private final Client client;
    private final Map<Integer, ClientChannel> readyChannels = new ConcurrentHashMap<>();
    private final CombineListener listener;
    private volatile boolean state = false;

    public DefaultProducer(String name, ProducerConfig config) {
        this(name, config, null);
    }

    public DefaultProducer(String name, ProducerConfig config, CombineListener clientListener) {
        this.name = name;
        this.config = Objects.requireNonNull(config, "Producer config not found");
        this.listener = clientListener == null ? new DefaultProducerListener(this) : clientListener;
        this.client = new Client(name, config.getClientConfig(), listener);
    }

    @Override
    public void start() {
        if (isRunning()) {
            if (logger.isWarnEnabled()) {
                logger.warn("Producer[{}] is running, don't run it replay", name);
            }
            return;
        }
        state = true;
        client.start();
    }

    private boolean isRunning() {
        return state;
    }

    @Override
    public MessageId send(String topic, String queue, ByteBuf message, Map<String, String> extras) {
        return send(topic, queue, message, extras, -1);
    }

    @Override
    public MessageId send(String topic, String queue, ByteBuf message, Map<String, String> extras, long timeout) {
        try {
            Promise<SendMessageResponse> promise = ImmediateEventExecutor.INSTANCE.newPromise();
            doSend(topic, queue, message, extras, config.getSendTimeoutMilliseconds(), promise);
            SendMessageResponse response = timeout > 0 ? promise.get(Math.max(timeout, 3_000L), TimeUnit.MILLISECONDS) : promise.get();
            return new MessageId(response.getLedger(), response.getEpoch(), response.getIndex());
        } catch (Throwable t) {
            throw new RemotingSendRequestException(
                    STR."Message send failed, topic[\{topic}] queue[\{queue}] length[\{ByteBufUtil.bufLength(
                            message)}]", t);
        } finally {
            ByteBufUtil.release(message);
        }
    }

    @Override
    public void sendAsync(String topic, String queue, ByteBuf message, Map<String, String> extras, SendCallback callback) {
        try {
            if (callback == null) {
                doSend(topic, queue, message, extras, config.getSendOnewayTimeoutMilliseconds(), null);
                return;
            }

            Promise<SendMessageResponse> promise = ImmediateEventExecutor.INSTANCE.newPromise();
            promise.addListener((GenericFutureListener<Future<SendMessageResponse>>) f -> {
                if (f.isSuccess()) {
                    SendMessageResponse res = f.getNow();
                    callback.onCompleted(new MessageId(res.getLedger(), res.getEpoch(), res.getIndex()), null);
                } else {
                    callback.onCompleted(null, f.cause());
                }
            });
            doSend(topic, queue, message, extras, config.getSendAsyncTimeoutMilliseconds(), promise);
        } catch (Throwable t) {
            throw new RemotingSendRequestException(
                    STR."Message send failed, topic[\{topic}] queue[\{queue}] length[\{ByteBufUtil.bufLength(
                            message)}]", t);
        } finally {
            ByteBufUtil.release(message);
        }
    }

    @Override
    public void sendOneway(String topic, String queue, ByteBuf message, Map<String, String> extras) {
        try {
            doSend(topic, queue, message, extras, config.getSendAsyncTimeoutMilliseconds(), null);
        } catch (Throwable t) {
            throw new RemotingSendRequestException(
                    STR."Message send oneway failed, topic[\{topic}] queue[\{queue}] length[\{ByteBufUtil.bufLength(
                            message)}]", t);
        } finally {
            ByteBufUtil.release(message);
        }
    }

    private void doSend(String topic, String queue, ByteBuf message, Map<String, String> extras, int timeoutMs, Promise<SendMessageResponse> promise) {
        TopicPatternUtil.validateQueue(queue);
        TopicPatternUtil.validateTopic(topic);

        MessageRouter router = client.fetchRouter(topic);
        if (router == null) {
            throw new IllegalStateException(STR."Message router[topic:\{topic}] not found");
        }

        MessageLedger ledger = router.routeLedger(queue);
        if (ledger == null) {
            throw new IllegalStateException(STR."Message ledger[queue:\{queue}] not found");
        }

        SocketAddress leader = ledger.leader();
        if (leader == null) {
            throw new IllegalStateException(STR."Message leader[topic:\{topic},queue:\{queue}] not found");
        }

        int marker = router.routeMarker(queue);
        SendMessageRequest request = SendMessageRequest.newBuilder().setLedger(ledger.id()).setMarker(marker).build();
        MessageMetadata metadata = buildMetadata(topic, queue, extras);
        ClientChannel channel = getReadyChannel(leader, ledger.id());
        channel.invoker().sendMessage(timeoutMs, promise, request, metadata, message);
    }

    private ClientChannel getReadyChannel(SocketAddress address, int ledger) {
        ClientChannel channel = readyChannels.get(ledger);
        if (channel != null && channel.isActive() && (address == null || channel.address().equals(address))) {
            return channel;
        }

        if (address == null) {
            throw new IllegalStateException(STR."Channel address not found, ledger[\{ledger}]");
        }

        synchronized (readyChannels) {
            channel = readyChannels.get(ledger);
            if (channel != null && channel.isActive() && channel.address().equals(address)) {
                return channel;
            }
            channel = client.getActiveChannel(address);
            readyChannels.put(ledger, channel);
            return channel;
        }
    }

    private MessageMetadata buildMetadata(String topic, String queue, Map<String, String> extras) {
        MessageMetadata.Builder builder = MessageMetadata.newBuilder().setTopic(topic).setQueue(queue);
        if (extras != null && !extras.isEmpty()) {
            for (Map.Entry<String, String> entry : extras.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                if (key != null && value != null) {
                    builder.putExtras(key, value);
                }
            }
        }
        return builder.build();
    }

    boolean containsRouter(String topic) {
        return client.containsRouter(topic);
    }

    MessageRouter fetchRouter(String topic) {
        return client.fetchRouter(topic);
    }

    void refreshRouter(String topic, ClientChannel channel) {
        client.refreshRouter(topic, channel);
    }

    String getName() {
        return name;
    }

    @Override
    public void close() {
        if (!isRunning()) {
            if (logger.isWarnEnabled()) {
                logger.warn("Producer[{}] was closed, don't execute it replay", name);
            }
            return;
        }

        state = false;
        try {
            listener.listenerCompleted();
        } catch (InterruptedException ignored) {
            // keep empty
        }
        client.close();
    }
}
