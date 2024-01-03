package org.meteor.client.producer;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import org.meteor.client.internal.*;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.common.thread.FastEventExecutor;
import org.meteor.remote.proto.client.TopicChangedSignal;

import javax.annotation.Nonnull;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class DefaultProducerListener implements ClientListener, MeterBinder {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(DefaultProducerListener.class);
    private static final String METRICS_NETTY_PENDING_TASK_NAME = "producer_netty_pending_task";
    private final DefaultProducer producer;
    private final EventExecutor refreshRouterExecutor;

    public DefaultProducerListener(DefaultProducer producer) {
        this.producer = producer;
        refreshRouterExecutor = new FastEventExecutor(new DefaultThreadFactory("client-producer-task"));
    }

    @Override
    public void onTopicChanged(ClientChannel channel, TopicChangedSignal signal) {
        String topic = signal.getTopic();
        if (!producer.containsRouter(topic)) {
            if (logger.isDebugEnabled()) {
                logger.debug("It's not contains the topic[{}] router", topic);
            }
            return;
        }

        int ledgerId = signal.getLedger();
        int version = signal.getLedgerVersion();
        refreshRouterExecutor.schedule(() -> {
            try {
                if (producer.containsRouter(topic)) {
                    MessageRouter router = producer.fetchRouter(topic);
                    if (router == null) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("The topic[{}] router is empty", topic);
                        }
                        return;
                    }

                    MessageLedger ledger = router.ledger(ledgerId);
                    if (ledger != null && version != 0 && ledger.version() >= version) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("The topic[{}] router is not need to refresh", topic);
                        }
                        return;
                    }
                    producer.refreshRouter(topic, channel);
                }
            } catch (Throwable t) {
                logger.debug(t.getMessage(), t);
            }
        }, ThreadLocalRandom.current().nextInt(5000), TimeUnit.MILLISECONDS);
    }

    @Override
    public void bindTo(@Nonnull MeterRegistry meterRegistry) {
        SingleThreadEventExecutor singleThreadEventExecutor = (SingleThreadEventExecutor) refreshRouterExecutor;
        Gauge.builder(METRICS_NETTY_PENDING_TASK_NAME, singleThreadEventExecutor, SingleThreadEventExecutor::pendingTasks)
                .tag("type", "producer-task")
                .tag("name", producer.getName())
                .tag("id", singleThreadEventExecutor.threadProperties().name())
                .register(meterRegistry);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Override
    public void listenerCompleted() {
        refreshRouterExecutor.shutdownGracefully();
        try {
            while (!refreshRouterExecutor.isTerminated()) {
                refreshRouterExecutor.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            logger.debug(e.getMessage(), e);
            Thread.currentThread().interrupt();
        }
    }
}
