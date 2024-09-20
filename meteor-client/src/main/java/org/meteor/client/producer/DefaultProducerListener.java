package org.meteor.client.producer;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.meteor.client.core.ClientChannel;
import org.meteor.client.core.CombineListener;
import org.meteor.client.core.MessageLedger;
import org.meteor.client.core.MessageRouter;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.common.thread.FastEventExecutor;
import org.meteor.remote.proto.client.TopicChangedSignal;

/**
 * The DefaultProducerListener class implements the CombineListener and MeterBinder interfaces
 * to handle various events within a client channel and to bind custom metrics to a MeterRegistry.
 * This class is responsible for reacting to topic changes and updating internal routing information.
 */
final class DefaultProducerListener implements CombineListener, MeterBinder {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(DefaultProducerListener.class);
    /**
     * Constant string name used for identifying the metric that tracks pending tasks
     * in Netty within the producer context. This metric helps in monitoring and
     * managing the Netty framework's task execution within the producer component,
     * allowing for better performance analysis and troubleshooting.
     */
    private static final String METRICS_NETTY_PENDING_TASK_NAME = "producer_netty_pending_task";
    /**
     * The producer instance used to produce records to the destination topic.
     * This is an immutable and final field within the DefaultProducerListener class.
     * It ensures consistency and prevents changing the producer instance once the DefaultProducerListener is constructed.
     */
    private final DefaultProducer producer;
    /**
     * An {@link EventExecutor} responsible for executing tasks related to refreshing
     * the router in the {@link DefaultProducerListener} class.
     * It ensures that routing updates are handled in a timely and efficient manner,
     * maintaining the necessary performance and responsiveness within the system
     * where routing changes occur.
     */
    private final EventExecutor refreshRouterExecutor;

    /**
     * Constructs a new DefaultProducerListener with the given producer.
     *
     * @param producer the DefaultProducer instance to associate with this listener
     */
    public DefaultProducerListener(DefaultProducer producer) {
        this.producer = producer;
        refreshRouterExecutor = new FastEventExecutor(new DefaultThreadFactory("client-producer-task"));
    }

    /**
     * Handles changes to the topic by refreshing the message router if needed.
     *
     * @param channel the client channel associated with the topic change
     * @param signal  the signal containing information about the topic change, such as the new topic and ledger information
     */
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

    /**
     * Binds the necessary metrics to the specified {@link MeterRegistry}.
     * This method registers a gauge metric for monitoring pending tasks in the
     * single-thread event executor associated with the refresh router executor.
     *
     * @param meterRegistry the registry to which the metrics will be bound, must not be null
     */
    @Override
    public void bindTo(@Nonnull MeterRegistry meterRegistry) {
        SingleThreadEventExecutor singleThreadEventExecutor = (SingleThreadEventExecutor) refreshRouterExecutor;
        Gauge.builder(METRICS_NETTY_PENDING_TASK_NAME, singleThreadEventExecutor, SingleThreadEventExecutor::pendingTasks)
                .tag("type", "producer-task")
                .tag("name", producer.getName())
                .tag("id", singleThreadEventExecutor.threadProperties().name())
                .register(meterRegistry);
    }

    /**
     * Signals that the listener has completed its tasks and will shut down the
     * {@code refreshRouterExecutor} gracefully.
     *
     * This method initiates a graceful shutdown of the {@code refreshRouterExecutor}
     * and waits for it to terminate. If the thread is interrupted while waiting for
     * termination, the interruption status is set, and the exception is logged.
     *
     * Uses:
     * <ul>
     * <li>{@code refreshRouterExecutor.shutdownGracefully()} to initiate the shutdown.
     * <li>{@code refreshRouterExecutor.awaitTermination(...)} to wait for termination.
     * <li>Catches {@link InterruptedException} to handle the interruption.
     * </ul>
     *
     * Logs:
     * <ul>
     * <li>Debug messages using the {@code logger} in case of interruption.
     * </ul>
     */
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
