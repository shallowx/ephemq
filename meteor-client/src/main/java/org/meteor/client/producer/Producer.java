package org.meteor.client.producer;

import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.*;
import org.meteor.client.internal.*;
import org.meteor.client.util.TopicPatternUtil;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.common.message.Extras;
import org.meteor.common.message.MessageId;
import org.meteor.remote.proto.MessageMetadata;
import org.meteor.remote.proto.server.SendMessageRequest;
import org.meteor.remote.proto.server.SendMessageResponse;
import org.meteor.remote.util.ByteBufUtil;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class Producer {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(Producer.class);
    private final String name;
    private final ProducerConfig config;
    private final Client client;
    private final Map<Integer, ClientChannel> ledgerChannels = new ConcurrentHashMap<>();

    private volatile Boolean state;
    private final ClientListener listener;

    public Producer(String name, ProducerConfig config) {
        this(name, config, null);
    }

    public Producer(String name, ProducerConfig config, ClientListener clientListener) {
        this.name = name;
        this.config = Objects.requireNonNull(config, "Producer config not found");
        this.listener = clientListener == null ? new DefaultProducerListener(this) : clientListener;
        this.client = new Client(name, config.getClientConfig(), listener);
    }

    public void start() {
        if (state != null) {
            if (logger.isWarnEnabled()) {
                logger.warn("Producer[{}] is running, don't run it replay", name);
            }
            return;
        }
        state = Boolean.TRUE;
        client.start();
    }

    public String getName() {
        return name;
    }

    public synchronized void close() {
        if (state != Boolean.TRUE) {
            if (logger.isWarnEnabled()) {
                logger.warn("Producer[{}] was closed, don't execute it replay", name);
            }
            return;
        }

        state = Boolean.FALSE;
        try {
            listener.listenerCompleted();
        } catch (InterruptedException ignored) {
            // keep empty
        }
        client.close();
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

    public MessageId send(String topic, String queue, ByteBuf message, Extras extras) {
        int length = ByteBufUtil.bufLength(message);
        try {
            Promise<SendMessageResponse> promise = ImmediateEventExecutor.INSTANCE.newPromise();
            doSend(topic, queue, message, extras, config.getSendTimeoutMs(), promise);
            SendMessageResponse response = promise.get();
            return new MessageId(response.getLedger(), response.getEpoch(), response.getIndex());
        } catch (Throwable t) {
            throw new RuntimeException(
                    String.format("Message send failed, topic[%s] queue[%s] length[%s]", topic, queue, length), t
            );
        } finally {
            ByteBufUtil.release(message);
        }
    }

    public void sendAsync(String topic, String queue, ByteBuf message, Extras extras, SendCallback callback) {
        int length = ByteBufUtil.bufLength(message);
        try {
            if (callback == null) {
                doSend(topic, queue, message, extras, config.getSendOnewayTimeoutMs(), null);
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
            doSend(topic, queue, message, extras, config.getSendAsyncTimeoutMs(), promise);
        } catch (Throwable t) {
            throw new RuntimeException(
                    String.format("Message async send failed, topic[%s] queue[%s] length[%s]", topic, queue, length), t
            );
        } finally {
            ByteBufUtil.release(message);
        }
    }

    public void sendOneway(String topic, String queue, ByteBuf message, Extras extras) {
        int length = ByteBufUtil.bufLength(message);
        try {
            doSend(topic, queue, message, extras, config.getSendAsyncTimeoutMs(), null);
        } catch (Throwable t) {
            throw new RuntimeException(
                    String.format("Message send oneway failed, topic[%s] queue[%s] length[%s]", topic, queue, length), t
            );
        } finally {
            ByteBufUtil.release(message);
        }
    }

    private void doSend(String topic, String queue, ByteBuf message, Extras extras, int timeoutMs, Promise<SendMessageResponse> promise) {
        TopicPatternUtil.validateQueue(queue);
        TopicPatternUtil.validateTopic(topic);

        MessageRouter router = client.fetchRouter(topic);
        if (router == null) {
            throw new IllegalStateException("Message router not found");
        }

        MessageLedger ledger = router.routeLedger(queue);
        if (ledger == null) {
            throw new IllegalStateException("Message ledger not found");
        }

        SocketAddress leader = ledger.leader();
        if (leader == null) {
            throw new IllegalStateException("Message leader not found");
        }

        int marker = router.routeMarker(queue);
        SendMessageRequest request = SendMessageRequest.newBuilder().setLedger(ledger.id()).setMarker(marker).build();
        MessageMetadata metadata = buildMetadata(topic, queue, extras);

        ClientChannel channel = fetchChannel(leader, ledger.id());
        channel.invoker().sendMessage(timeoutMs, promise, request, metadata, message);
    }

    private ClientChannel fetchChannel(SocketAddress address, int ledger) {
        ClientChannel channel = ledgerChannels.get(ledger);
        if (channel != null && channel.isActive() && (address == null || channel.address().equals(address))) {
            return channel;
        }

        if (address == null) {
            throw new IllegalStateException("Channel address not found, ledger[" + ledger + "]");
        }

        synchronized (ledgerChannels) {
            channel = ledgerChannels.get(ledger);
            if (channel != null && channel.isActive() && channel.address().equals(address)) {
                return channel;
            }
            channel = client.fetchChannel(address);
            ledgerChannels.put(ledger, channel);
            return channel;
        }
    }

    private MessageMetadata buildMetadata(String topic, String queue, Extras extras) {
        MessageMetadata.Builder builder = MessageMetadata.newBuilder().setTopic(topic).setQueue(queue);
        if (extras != null) {
            for (Map.Entry<String, String> entry : extras) {
                String key = entry.getKey();
                String value = entry.getValue();
                if (key != null && value != null) {
                    builder.putExtras(key, value);
                }
            }
        }
        return builder.build();
    }
}
