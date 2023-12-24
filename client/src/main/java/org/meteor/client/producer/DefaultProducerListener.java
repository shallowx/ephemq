package org.meteor.client.producer;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import org.meteor.client.internal.*;
import org.meteor.common.thread.FastEventExecutor;
import org.meteor.remote.proto.client.TopicChangedSignal;

import javax.annotation.Nonnull;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class DefaultProducerListener implements ClientListener, MeterBinder {
    private static final String METRICS_NETTY_PENDING_TASK_NAME = "producer_netty_pending_task";
    private final Producer producer;
    private final EventExecutor executor;

    public DefaultProducerListener(Producer producer) {
        this.producer = producer;
        executor = new FastEventExecutor(new DefaultThreadFactory("client-producer-task"));
    }

    @Override
    public void onTopicChanged(ClientChannel channel, TopicChangedSignal signal) {
        String topic = signal.getTopic();
        if (!producer.containsRouter(topic)) {
            return;
        }

        int ledgerId = signal.getLedger();
        int version = signal.getLedgerVersion();
        executor.schedule(() -> {
            try {
                if (producer.containsRouter(topic)) {
                    MessageRouter router = producer.fetchRouter(topic);
                    if (router == null) {
                        return;
                    }

                    MessageLedger ledger = router.ledger(ledgerId);
                    if (ledger != null && version != 0 && ledger.version() >= version) {
                        return;
                    }
                    producer.refreshRouter(topic, channel);
                }
            } catch (Throwable ignored) {
            }
        }, ThreadLocalRandom.current().nextInt(5000), TimeUnit.MILLISECONDS);
    }

    @Override
    public void bindTo(@Nonnull MeterRegistry meterRegistry) {
        SingleThreadEventExecutor singleThreadEventExecutor = (SingleThreadEventExecutor) executor;
        Gauge.builder(METRICS_NETTY_PENDING_TASK_NAME, singleThreadEventExecutor, SingleThreadEventExecutor::pendingTasks)
                .tag("type", "producer-task")
                .tag("name", producer.getName())
                .tag("id", singleThreadEventExecutor.threadProperties().name())
                .register(meterRegistry);
    }

    @Override
    public void listenerCompleted() {
        executor.shutdownGracefully();
        try {
            while (!executor.isTerminated()) {
                executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
