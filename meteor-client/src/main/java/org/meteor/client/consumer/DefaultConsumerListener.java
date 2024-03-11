package org.meteor.client.consumer;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.netty.buffer.ByteBuf;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import org.meteor.client.internal.ClientChannel;
import org.meteor.client.internal.CombineListener;
import org.meteor.client.internal.MessageLedger;
import org.meteor.client.internal.MessageRouter;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.common.message.MessageId;
import org.meteor.remote.proto.MessageMetadata;
import org.meteor.remote.proto.client.MessagePushSignal;
import org.meteor.remote.proto.client.NodeOfflineSignal;
import org.meteor.remote.proto.client.TopicChangedSignal;
import org.meteor.remote.util.NetworkUtil;
import org.meteor.remote.util.ProtoBufUtil;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

final class DefaultConsumerListener implements CombineListener, MeterBinder {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(DefaultConsumerListener.class);
    private static final String METRICS_NETTY_PENDING_TASK_NAME = "consumer_netty_pending_task";
    private final DefaultConsumer consumer;
    private final MessageHandler[] handlers;
    private final IntObjectMap<MessageHandler> markerOfHandlers;
    private final ConsumerConfig consumerConfig;
    private final EventExecutorGroup group;
    private final Map<String, Future<?>> obsoleteFutures = new ConcurrentHashMap<>();

    public DefaultConsumerListener(Consumer consumer, ConsumerConfig consumerConfig, MessageListener listener) {
        this.consumer = (DefaultConsumer) consumer;
        this.consumerConfig = consumerConfig;

        int shardCount = Math.max(consumerConfig.getHandlerThreadLimit(), consumerConfig.getHandlerShardLimit());
        this.handlers = new MessageHandler[shardCount];
        int handlerPendingCount = consumerConfig.getHandlerPendingLimit();
        this.group = NetworkUtil.newEventExecutorGroup(consumerConfig.getHandlerThreadLimit(), "consumer-message-group");
        for (int i = 0; i < shardCount; i++) {
            Semaphore semaphore = new Semaphore(handlerPendingCount);
            MessageHandler messageHandler = new MessageHandler(String.valueOf(i), semaphore, group.next(), this.consumer.getSubscribeShips(), listener);
            this.handlers[i] = messageHandler;
        }
        this.markerOfHandlers = new IntObjectHashMap<>(shardCount);
    }

    @Override
    public void onChannelClosed(ClientChannel channel) {
        consumer.touchChangedTask();
    }

    @Override
    public void onTopicChanged(ClientChannel channel, TopicChangedSignal signal) {
        String topic = signal.getTopic();
        if (!consumer.containsRouter(topic)) {
            if (logger.isDebugEnabled()) {
                logger.debug("The consumer listener doesn't contains topic[{}] message router", topic);
            }
            return;
        }

        int ledgerId = signal.getLedger();
        int ledgerVersion = signal.getLedgerVersion();
        consumer.getExecutor().schedule(() -> {
            try {
                if (consumer.containsRouter(topic)) {
                    MessageRouter router = consumer.fetchRouter(topic);
                    if (router == null) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("The consumer listener that topic[{}] message router is empty", topic);
                        }
                        return;
                    }

                    MessageLedger ledger = router.ledger(ledgerId);
                    if (ledger == null || ledgerVersion == 0 || ledger.version() < ledgerVersion) {
                        consumer.refreshRouter(topic, channel);
                    }

                    consumer.touchChangedTask();
                }
            } catch (Throwable t) {
                logger.error(t);
            }
        }, ThreadLocalRandom.current().nextInt(5000), TimeUnit.MILLISECONDS);
    }

    @Override
    public void onPushMessage(ClientChannel channel, MessagePushSignal signal, ByteBuf data) {
        int ledger = signal.getLedger();
        int marker = signal.getMarker();
        int epoch = signal.getEpoch();
        long index = signal.getIndex();
        if (!channel.equals(consumer.getReadyChannels().get(ledger))) {
            try {
                MessageMetadata metadata = ProtoBufUtil.readProto(data, MessageMetadata.parser());
                String topic = metadata.getTopic();
                obsoleteFutures.computeIfAbsent(ledger + "@" + channel.id(),
                        k -> consumer.getExecutor().schedule(() -> {
                            if (channel.isActive() && !channel.equals(consumer.getReadyChannels().get(ledger))) {
                                consumer.doCleanSubscribe(channel, topic, ledger);
                            }

                            obsoleteFutures.remove(k);
                        }, consumerConfig.getControlRetryDelayMilliseconds(), TimeUnit.MILLISECONDS));
            } catch (Throwable t) {
                logger.error(t);
            }
            return;
        }

        AtomicReference<MessageId> sequence = consumer.getLedgerSequences().get(ledger);
        if (sequence == null) {
            return;
        }

        MessageId id = new MessageId(ledger, epoch, index);
        while (true) {
            MessageId lastId = sequence.get();
            if (lastId == null || (epoch == lastId.epoch() && index > lastId.index() || epoch > lastId.epoch())) {
                if (sequence.compareAndSet(lastId, id)) {
                    break;
                }
            } else {
                return;
            }
        }

        try {
            MessageHandler handler = getHandler(marker, handlers.length);
            handler.handle(channel, marker, id, data);
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("The consumer listener that handle message failure", t);
            }
        }
    }

    private MessageHandler getHandler(int marker, int length) {
        return markerOfHandlers.computeIfAbsent(marker, v -> handlers[consistentHash(marker, length)]);
    }

    private int consistentHash(int input, int buckets) {
        long state = input & 0xffffffffL;
        int candidate = 0;
        int next;
        while (true) {
            next = (int) ((candidate + 1) / (((double) ((int) ((state = (2862933555777941757L * state + 1)) >>> 33) + 1)) / 0x1.0p31));
            if (next > 0 && next < buckets) {
                candidate = next;
            } else {
                return candidate;
            }
        }
    }

    @Override
    public void onNodeOffline(ClientChannel channel, NodeOfflineSignal signal) {
        CombineListener.super.onNodeOffline(channel, signal);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Override
    public void listenerCompleted() throws InterruptedException {
        if (group != null) {
            group.shutdownGracefully().sync();
            Iterator<EventExecutor> iterator = group.iterator();
            while (iterator.hasNext()) {
                try {
                    EventExecutor next = iterator.next();
                    while (!next.isTerminated()) {
                        next.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
                    }
                } catch (Exception e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    @Override
    public void bindTo(@Nonnull MeterRegistry meterRegistry) {
        SingleThreadEventExecutor singleThreadEventExecutor = (SingleThreadEventExecutor) consumer.getExecutor();
        registerMetrics("consumer-task", meterRegistry, singleThreadEventExecutor);
        for (EventExecutor eventExecutor : group) {
            try {
                SingleThreadEventExecutor executor = (SingleThreadEventExecutor) eventExecutor;
                registerMetrics("consumer-handler", meterRegistry, executor);
            } catch (Throwable ignored) {
            }
        }
    }

    private void registerMetrics(String type, MeterRegistry meterRegistry, SingleThreadEventExecutor singleThreadEventExecutor) {
        Gauge.builder(METRICS_NETTY_PENDING_TASK_NAME, singleThreadEventExecutor, SingleThreadEventExecutor::pendingTasks)
                .tag("type", type)
                .tag("name", consumer.getName())
                .tag("id", singleThreadEventExecutor.threadProperties().name())
                .register(meterRegistry);
    }
}
