package org.ostara.client.producer;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.*;
import org.ostara.client.internal.*;
import org.ostara.client.util.TopicPatterns;
import org.ostara.common.Extras;
import org.ostara.common.MessageId;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.common.thread.FastEventExecutor;
import org.ostara.remote.proto.MessageMetadata;
import org.ostara.remote.proto.client.TopicChangedSignal;
import org.ostara.remote.proto.server.SendMessageRequest;
import org.ostara.remote.proto.server.SendMessageResponse;
import org.ostara.remote.util.ByteBufUtils;

import javax.annotation.Nonnull;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class Producer implements MeterBinder {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(Producer.class);
    private static final String METRICS_NETTY_PENDING_TASK_NAME = "producer_netty_pending_task";
    private final String name;
    private final ProducerConfig config;
    private final Client client;
    private final Map<Integer, ClientChannel> ledgerChannels = new ConcurrentHashMap<>();
    private EventExecutor executor;
    private volatile Boolean state;

    public Producer(String name, ProducerConfig config) {
        this.name = name;
        this.config = Objects.requireNonNull(config, "Producer config not found");
        this.client = new Client(name, config.getClientConfig(), new ProducerListener());
    }

    public void start() {
        if (state != null) {
            return;
        }
        state = Boolean.TRUE;
        client.start();

        executor = new FastEventExecutor(new DefaultThreadFactory("client-producer-task"));
    }

    public synchronized void close() throws InterruptedException {
        if (state != Boolean.TRUE) {
            return;
        }

        state = Boolean.FALSE;
        if (executor != null) {
            executor.shutdownGracefully();
            try {
                while (!executor.isTerminated()) {
                    executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
                }
            } catch (InterruptedException e) {
                // Let the caller handle the interruption.
                Thread.currentThread().interrupt();
            }
        }

        client.close();
    }

    public MessageId send(String topic, String queue, ByteBuf message, Extras extras) {
        int length = ByteBufUtils.bufLength(message);
        try {
            Promise<SendMessageResponse> promise = ImmediateEventExecutor.INSTANCE.newPromise();
            doSend(topic, queue, message, extras, config.getSendTimeoutMs(), promise);
            SendMessageResponse response = promise.get();

            return new MessageId(response.getLedger(), response.getEpoch(), response.getIndex());
        } catch (Throwable t) {
            throw new RuntimeException(
                    String.format("Message send failed, topic=%s queue=%s length=%s", topic, queue, length), t
            );
        } finally {
            ByteBufUtils.release(message);
        }
    }

    public void sendAsync(String topic, String queue, ByteBuf message, Extras extras, SendCallback callback) {
        int length = ByteBufUtils.bufLength(message);
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
                    String.format("Message async send failed, topic=%s queue=%s length=%s", topic, queue, length), t
            );
        } finally {
            ByteBufUtils.release(message);
        }
    }

    public void sendOneway(String topic, String queue, ByteBuf message, Extras extras) {
        int length = ByteBufUtils.bufLength(message);
        try {
            doSend(topic, queue, message, extras, config.getSendAsyncTimeoutMs(), null);
        } catch (Throwable t) {
            throw new RuntimeException(
                    String.format("Message send oneway failed, topic=%s queue=%s length=%s", topic, queue, length), t
            );
        } finally {
            ByteBufUtils.release(message);
        }
    }

    private void doSend(String topic, String queue, ByteBuf message, Extras extras, int timeoutMs, Promise<SendMessageResponse> promise) {
        TopicPatterns.validateQueue(queue);
        TopicPatterns.validateTopic(topic);

        MessageRouter router = client.fetchMessageRouter(topic);
        if (router == null) {
            throw new IllegalStateException("Message router not found");
        }

        MessageLedger ledger = router.calculateLedger(queue);
        if (ledger == null) {
            throw new IllegalStateException("Message ledger not found");
        }

        SocketAddress leader = ledger.leader();
        if (leader == null) {
            throw new IllegalStateException("Message leader not found");
        }

        int marker = router.calculateMarker(queue);
        SendMessageRequest request = SendMessageRequest.newBuilder().setLedger(ledger.id()).setMarker(marker).build();
        MessageMetadata metadata = assembleMetadata(topic, queue, extras);

        ClientChannel channel = fetchChannel(leader, ledger.id());
        channel.invoker().sendMessage(timeoutMs, promise, request, metadata, message);
    }

    private ClientChannel fetchChannel(SocketAddress address, int ledger) {
        ClientChannel channel = ledgerChannels.get(ledger);
        if (channel != null && channel.isActive() && (address == null || channel.address().equals(address))) {
            return channel;
        }

        if (address == null) {
            throw new IllegalStateException("Channel address not found, ledger=" + ledger);
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

    private MessageMetadata assembleMetadata(String topic, String queue, Extras extras) {
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

    @Override
    public void bindTo(@Nonnull MeterRegistry meterRegistry) {
        SingleThreadEventExecutor singleThreadEventExecutor = (SingleThreadEventExecutor) executor;
        Gauge.builder(METRICS_NETTY_PENDING_TASK_NAME, singleThreadEventExecutor, SingleThreadEventExecutor::pendingTasks)
                .tag("type", "producer-task")
                .tag("name", name)
                .tag("id", singleThreadEventExecutor.threadProperties().name())
                .register(meterRegistry);
    }

    private class ProducerListener implements ClientListener {
        @Override
        public void onTopicChanged(ClientChannel channel, TopicChangedSignal signal) {
            String topic = signal.getTopic();
            if (!client.containsMessageRouter(topic)) {
                return;
            }

            int ledgerId = signal.getLedger();
            int version = signal.getLedgerVersion();
            executor.schedule(() -> {
                try {
                    if (client.containsMessageRouter(topic)) {
                        MessageRouter router = client.fetchMessageRouter(topic);
                        if (router == null) {
                            return;
                        }

                        MessageLedger ledger = router.ledger(ledgerId);
                        if (ledger != null && version != 0 && ledger.version() >= version) {
                            return;
                        }
                        client.refreshMessageRouter(topic, channel);
                    }
                } catch (Throwable ignored) {
                }
            }, ThreadLocalRandom.current().nextInt(5000), TimeUnit.MILLISECONDS);
        }
    }
}
