package org.ephemq.listener;

import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.binder.jvm.*;
import io.micrometer.core.instrument.binder.system.FileDescriptorMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import io.netty.util.internal.StringUtil;
import org.ephemq.common.logging.InternalLogger;
import org.ephemq.common.logging.InternalLoggerFactory;
import org.ephemq.common.message.Node;
import org.ephemq.common.message.TopicAssignment;
import org.ephemq.common.message.TopicPartition;
import org.ephemq.config.CommonConfig;
import org.ephemq.config.MetricsConfig;
import org.ephemq.ledger.Log;
import org.ephemq.metrics.config.MeteorPrometheusRegistry;
import org.ephemq.metrics.config.MetricsRegistrySetUp;
import org.ephemq.metrics.jvm.DefaultJVMInfoMetrics;
import org.ephemq.metrics.jvm.JmxMetricsRegistry;
import org.ephemq.metrics.netty.NettyMetrics;
import org.ephemq.support.Manager;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.ephemq.metrics.config.MetricsConstants.*;

/**
 * MetricsListener is responsible for gathering and reporting various metrics
 * related to API commands, server activities, log operations, and message topics.
 * It implements several listener interfaces to capture and handle different types of events.
 */
public class MetricsListener implements APIListener, ServerListener, LogListener, TopicListener, AutoCloseable {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MetricsListener.class);
    /**
     * A meter registry used to collect and manage various metrics.
     * This registry is globally accessible and shared across the application.
     */
    private final MeterRegistry registry = Metrics.globalRegistry;
    /**
     * Common configuration instance used to retrieve and manage common
     * configurations required by the MetricsListener.
     * <p>
     * This configuration is immutable after being set through the constructor,
     * ensuring consistent configuration throughout the lifecycle of the MetricsListener object.
     */
    private final CommonConfig config;
    /**
     * The manager responsible for handling various operations and services within the
     * MetricsListener class.
     * <p>
     * This manager provides functionality such as starting and shutting down services,
     * handling topics, managing clusters, dealing with logs, establishing connections,
     * adding listeners, and providing executor groups for handling different types of events.
     */
    private final Manager manager;
    /**
     * A map to store counters for received messages per topic.
     * Each key represents the unique identifier for a topic,
     * and each value is a Counter object that tracks the count
     * of received messages for the corresponding topic.
     */
    private final Map<Integer, Counter> topicReceiveCounters = new ConcurrentHashMap<>();
    /**
     * A concurrent map to store counters for tracking the number of push messages per topic.
     * <p>
     * Each entry in the map associates a topic code with its corresponding {@link Counter} object.
     * This map ensures that updating the counters is thread-safe in a concurrent environment.
     * <p>
     * The counters are used to monitor and measure push message activities for different topics,
     * providing a mechanism to collect metrics for push operations.
     */
    private final Map<Integer, Counter> topicPushCounters = new ConcurrentHashMap<>();
    /**
     * A map that tracks the success counters of various request types.
     * The key represents the unique identifier of the request type, and the value
     * is a {@link Counter} that keeps count of successful requests for that type.
     * This map is thread-safe to allow concurrent access and modification.
     */
    private final Map<Integer, Counter> requestSuccesses = new ConcurrentHashMap<>();
    /**
     * A map that tracks the number of request failures categorized by their error codes.
     * The map's key represents the error code, and the value is a `Counter` that aggregates the
     * number of occurrences of that specific failure.
     */
    private final Map<Integer, Counter> requestFailures = new ConcurrentHashMap<>();
    /**
     * A map that holds a summary of request sizes for different request codes.
     * <p>
     * This map uses an integer as the key, representing the request code,
     * and the value is a DistributionSummary that provides statistical information
     * about the sizes of the requests received for each code.
     * <p>
     * The map is implemented as a ConcurrentHashMap to ensure thread-safe operations
     * when multiple threads are accessing and updating the request size summaries.
     */
    private final Map<Integer, DistributionSummary> requestSizeSummary = new ConcurrentHashMap<>();
    /**
     * A map that holds the distribution summaries of request times, categorized by HTTP status codes.
     * Each key is an integer representing a specific HTTP status code, and each value is a
     * {@link DistributionSummary} that records and summarizes the distribution of request times
     * for that status code.
     * <p>
     * This map is thread-safe and supports concurrent access and modification.
     */
    private final Map<Integer, DistributionSummary> requestTimesSummary = new ConcurrentHashMap<>();
    /**
     * An atomic integer counter to keep track of the number of partitions.
     * <p>
     * This variable is used within the MetricsListener to monitor
     * partition-related metrics and maintain concurrency safety.
     */
    private final AtomicInteger partitionCounts = new AtomicInteger();
    /**
     * Tracks the number of partitions for which the current instance is the leader.
     * This variable is an atomic integer, ensuring thread-safe operations for incrementing and decrementing
     * the count of partition leaderships within a concurrent environment.
     */
    private final AtomicInteger partitionLeaderCounts = new AtomicInteger();
    /**
     * Represents the sample size for metrics collection.
     * This value is used to determine the number of samples to be collected
     * for a specific metric within the MetricsListener.
     */
    private final int metricsSample;
    /**
     * An instance of MetricsRegistrySetUp responsible for setting up and shutting down the
     * metrics registry. This is used within the MetricsListener class to manage the lifecycle
     * of metrics-related configurations and resources.
     */
    private final MetricsRegistrySetUp meterRegistrySetup;
    /**
     * An instance of JvmGcMetrics used for monitoring JVM garbage collection metrics.
     * <p>
     * This variable holds an object that provides detailed metrics related to the JVM's garbage collection process.
     * It is part of the MetricsListener class, which is responsible for handling various metrics within the system.
     * The collected garbage collection metrics can be utilized for monitoring and analysis to ensure optimal performance.
     */
    private final JvmGcMetrics jvmGcMetrics;

    /**
     * A FastThreadLocal variable that tracks the count of sampled metrics in the MetricsListener class.
     * This variable is initialized to zero and is used to maintain a thread-local count of metrics samples.
     */
    private final FastThreadLocal<Integer> metricsSampleCount = new FastThreadLocal<>() {
        @Override
        protected Integer initialValue() throws Exception {
            return 0;
        }
    };

    /**
     * Constructs a new {@code MetricsListener} with the specified properties, configuration, and manager.
     *
     * @param properties Properties for configuring the metrics.
     * @param config Common configuration.
     * @param metricsConfiguration Configuration specific to metrics.
     * @param manager Manager responsible for handling metrics-related operations.
     */
    public MetricsListener(Properties properties, CommonConfig config, MetricsConfig metricsConfiguration,
                           Manager manager) {
        this.config = config;
        this.manager = manager;
        this.metricsSample = metricsConfiguration.getMetricsSampleLimit();
        this.meterRegistrySetup = new MeteorPrometheusRegistry();
        this.meterRegistrySetup.setUp(properties);

        MetricsRegistrySetUp jmxMeterRegistrySetup = new JmxMetricsRegistry();
        jmxMeterRegistrySetup.setUp(properties);


        io.micrometer.core.instrument.Tags tags = Tags.of(CLUSTER_TAG, config.getClusterName()).and(BROKER_TAG, config.getClusterName());
        new DefaultJVMInfoMetrics(tags).bindTo(registry);
        new UptimeMetrics(tags).bindTo(registry);
        new FileDescriptorMetrics(tags).bindTo(registry);
        new ClassLoaderMetrics(tags).bindTo(registry);
        new JvmMemoryMetrics(tags).bindTo(registry);

        this.jvmGcMetrics = new JvmGcMetrics(tags);
        this.jvmGcMetrics.bindTo(registry);

        new JvmCompilationMetrics(tags).bindTo(registry);
        new JvmInfoMetrics().bindTo(registry);
        new JvmThreadMetrics(tags).bindTo(registry);
        new ProcessorMetrics(tags).bindTo(registry);
        new NettyMetrics(tags).bindTo(registry);

        Gauge.builder(TOPIC_PARTITION_COUNT_GAUGE_NAME, partitionCounts, AtomicInteger::doubleValue)
                .tags(Tags.of(CLUSTER_TAG, config.getClusterName()).and(BROKER_TAG, config.getServerId()))
                .register(registry);

        Gauge.builder(TOPIC_PARTITION_LEADER_COUNT_GAUGE_NAME, partitionLeaderCounts, AtomicInteger::doubleValue)
                .tags(Tags.of(CLUSTER_TAG, config.getClusterName()).and(BROKER_TAG, config.getServerId()))
                .register(registry);
    }

    /**
     * Closes the resources held by this MetricsListener instance.
     * This method ensures that `jvmGcMetrics` and `meterRegistrySetup`
     * are properly closed and shut down, respectively.
     * If any exceptions occur during this process, they are logged using the `logger`.
     *
     * @throws Exception if an error occurs during the closure of resources.
     */
    @Override
    public void close() throws Exception {
        try {
            this.jvmGcMetrics.close();
            this.meterRegistrySetup.shutdown();
        } catch (Throwable t) {
            logger.error(t.getMessage(), t);
        }
    }

    /**
     * Handles the execution of a command by updating metrics related to the command.
     *
     * @param code The numerical code identifying the type of command executed.
     * @param bytes The number of bytes processed during the execution of the command.
     * @param cost The time taken to execute the command, in nanoseconds.
     * @param ret A boolean indicating whether the command was successful (true) or failed (false).
     */
    @Override
    public void onCommand(int code, int bytes, long cost, boolean ret) {
        try {
            Counter counter = getCounter(code, ret);
            counter.increment();
            Integer sample = metricsSampleCount.get();
            metricsSampleCount.set(sample + 1);
            if (sample > metricsSample) {
                metricsSampleCount.set(0);
                DistributionSummary drs = requestSizeSummary.computeIfAbsent(code, s -> buildSummary(REQUEST_SIZE_SUMMARY_NAME, code, "bytes"));
                drs.record(bytes);

                DistributionSummary drt = requestTimesSummary.computeIfAbsent(code, s -> buildSummary(API_RESPONSE_TIME_NAME, code, "ns"));
                drt.record(bytes);
            }
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("Metrics command listener failed, code={}", code, t);
            }
        }
    }

    /**
     * Builds a DistributionSummary metric with the specified name, code, and unit.
     *
     * @param name the name of the DistributionSummary metric
     * @param code the code identifying the command or type related to the metric
     * @param unit the base unit for measuring the metric values, e.g., "bytes" or "ns"
     * @return a configured DistributionSummary instance
     */
    private DistributionSummary buildSummary(String name, int code, String unit) {
        return DistributionSummary.builder(name)
                .tags(Tags.of(TYPE_TAG, String.valueOf(code))
                        .and(BROKER_TAG, config.getServerId())
                        .and(CLUSTER_TAG, config.getClusterName()))
                .baseUnit(unit)
                .distributionStatisticExpiry(Duration.ofSeconds(30))
                .publishPercentiles(0.99, 0.999, 0.9)
                .register(registry);
    }

    /**
     * Retrieves or initializes a Counter based on the given code and result status.
     * If the counter associated with the provided code and result status does not exist,
     * it will be created and registered.
     *
     * @param code the code identifying the specific counter
     * @param ret flag indicating the result status; true for request successes, false for request failures
     * @return the Counter associated with the given code and result status
     */
    private Counter getCounter(int code, boolean ret) {
        Map<Integer, Counter> counters = ret ? requestSuccesses : requestFailures;
        Counter counter = counters.get(code);
        if (counter == null) {
            counter = counters.computeIfAbsent(code,
                    s -> Counter.builder(REQUEST_STATE_COUNTER_NAME)
                            .tags(Tags.of(TYPE_TAG, String.valueOf(code))
                                    .and(BROKER_TAG, config.getServerId())
                                    .and(CLUSTER_TAG, config.getClusterName())
                                    .and(RESULT_TAG, ret ? "success" : "failure"))
                            .register(registry));
        }
        return counter;
    }

    /**
     * Handles the initialization of a logging mechanism when the MetricsListener is set up.
     *
     * @param log the Log instance that provides logging capabilities for tracking and recording events.
     */
    @Override
    public void onInitLog(Log log) {

    }

    /**
     * Handles the receipt of a message by updating relevant metrics.
     *
     * @param topic The topic to which the received message belongs.
     * @param ledger The ledger ID associated with the message.
     * @param count The number of messages received.
     */
    @Override
    public void onReceiveMessage(String topic, int ledger, int count) {
        try {
            Counter counter = topicReceiveCounters.get(ledger);
            if (counter == null) {
                counter = topicReceiveCounters.computeIfAbsent(ledger, metrics ->
                        Counter.builder(TOPIC_MSG_RECEIVE_COUNTER_NAME)
                                .tag(TOPIC_TAG, topic)
                                .tag(LEDGER_TAG, String.valueOf(ledger))
                                .tag(CLUSTER_TAG, config.getClusterName())
                                .tag(BROKER_TAG, config.getServerId())
                                .tag(TYPE_TAG, "send")
                                .register(Metrics.globalRegistry)
                );
            }
            counter.increment(count);
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("Metrics receive message listener failed, topic={} ledger={} count={}", topic, ledger,
                        count, t);
            }
        }
    }

    /**
     * Handles synchronization messages for a specific topic and ledger by updating the metrics counters.
     *
     * @param topic the topic associated with the synchronization message
     * @param ledger the identifier of the ledger related to the message
     * @param count the number of messages to be synchronized
     */
    @Override
    public void onSyncMessage(String topic, int ledger, int count) {
        try {
            Counter counter = topicReceiveCounters.get(ledger);
            if (counter == null) {
                counter = topicReceiveCounters.computeIfAbsent(ledger, metrics ->
                        Counter.builder(TOPIC_MSG_RECEIVE_COUNTER_NAME)
                                .tag(TOPIC_TAG, topic)
                                .tag(LEDGER_TAG, String.valueOf(ledger))
                                .tag(CLUSTER_TAG, config.getClusterName())
                                .tag(BROKER_TAG, config.getServerId())
                                .tag(TYPE_TAG, "sync")
                                .register(Metrics.globalRegistry)
                );
            }
            counter.increment(count);
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("Metrics sync message listener failed, topic={} ledger={} count={}", topic, ledger, count,
                        t);
            }
        }
    }

    /**
     * Handles push messages related to a specific topic and ledger.
     *
     * @param topic the topic associated with the push message
     * @param ledger the ledger ID associated with the topic
     * @param count the number of messages being pushed
     */
    @Override
    public void onPushMessage(String topic, int ledger, int count) {
        try {
            Counter counter = topicPushCounters.get(ledger);
            if (counter == null) {
                counter = topicPushCounters.computeIfAbsent(ledger, k ->
                        Counter.builder(TOPIC_MSG_PUSH_COUNTER_NAME)
                                .tag(TOPIC_TAG, topic)
                                .tag(LEDGER_TAG, String.valueOf(ledger))
                                .tag(CLUSTER_TAG, config.getClusterName())
                                .tag(BROKER_TAG, config.getServerId())
                                .tag(TYPE_TAG, "single").register(registry)
                );
            }
            counter.increment(count);
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("Metrics push message listener failed, topic={} ledger={} count={}", topic, ledger, count,
                        t);
            }
        }
    }

    /**
     * Handles messages when a chunk of log entries is pushed.
     *
     * @param topic The topic of the log entries.
     * @param ledger The ledger ID where the log entries are stored.
     * @param count The number of log entries in the chunk.
     */
    @Override
    public void onChunkPushMessage(String topic, int ledger, int count) {
    }

    /**
     * Initializes and registers metrics for different types of event executors when the node starts up.
     *
     * @param node the node that is starting up, encapsulating configuration details such as ID, host, port, and cluster information
     */
    @Override
    public void onStartup(Node node) {
        String clusterName = config.getClusterName();
        String serverId = config.getServerId();

        // storage metrics
        for (EventExecutor executor : manager.getMessageStorageEventExecutorGroup()) {
            try {
                SingleThreadEventExecutor se = (SingleThreadEventExecutor) executor;
                Gauge.builder(NETTY_PENDING_TASK_NAME, se, SingleThreadEventExecutor::pendingTasks)
                        .tag(CLUSTER_TAG, clusterName)
                        .tag(BROKER_TAG, serverId)
                        .tag(TYPE_TAG, "storage")
                        .tag(NAME, StringUtil.EMPTY_STRING)
                        .tag(ID, se.threadProperties().name()).register(registry);
            } catch (Throwable t) {
                if (logger.isErrorEnabled()) {
                    logger.error("Storage metrics started failed, cluster={} server_id={} executor={}", clusterName, serverId, executor.toString(), t);
                }
            }
        }

        // dispatch metrics
        for (EventExecutor executor : manager.getMessageDispatchEventExecutorGroup()) {
            try {
                SingleThreadEventExecutor se = (SingleThreadEventExecutor) executor;
                Gauge.builder(NETTY_PENDING_TASK_NAME, se, SingleThreadEventExecutor::pendingTasks)
                        .tag(CLUSTER_TAG, clusterName)
                        .tag(BROKER_TAG, serverId)
                        .tag(TYPE_TAG, "dispatch")
                        .tag(NAME, StringUtil.EMPTY_STRING)
                        .tag(ID, se.threadProperties().name()).register(registry);
            } catch (Throwable t) {
                if (logger.isErrorEnabled()) {
                    logger.error("Dispatch metrics started failed, cluster={} server_id={} executor={}", clusterName, serverId, executor.toString(), t);
                }
            }
        }

        // command metrics
        for (EventExecutor executor : manager.getCommandHandleEventExecutorGroup()) {
            try {
                SingleThreadEventExecutor se = (SingleThreadEventExecutor) executor;
                Gauge.builder(NETTY_PENDING_TASK_NAME, se, SingleThreadEventExecutor::pendingTasks)
                        .tag(CLUSTER_TAG, clusterName)
                        .tag(BROKER_TAG, serverId)
                        .tag(TYPE_TAG, "command")
                        .tag(NAME, StringUtil.EMPTY_STRING)
                        .tag(ID, se.threadProperties().name()).register(registry);
            } catch (Throwable t) {
                if (logger.isErrorEnabled()) {
                    logger.error("Command metrics started failed, cluster={} server_id={} executor={}", clusterName, serverId, executor.toString(), t);
                }
            }
        }
    }

    /**
     * Invoked when the server shuts down, allowing for custom behavior to be executed during
     * the shutdown process. This can be used to perform tasks such as resource cleanup or
     * logging shutdown events.
     *
     * @param node the Node instance representing the server that is shutting down, providing
     *             details about the server instance such as its id, host, port, cluster, and
     *             other metadata.
     */
    @Override
    public void onShutdown(Node node) {
    }

    /**
     * Handles the initialization of a partition for a given topic and ledger.
     * Increments the partition count metric when a partition is initialized.
     * Logs an error if the metric update fails.
     *
     * @param topicPartition the partition of the topic being initialized
     * @param ledger the ID of the ledger associated with the partition
     */
    @Override
    public void onPartitionInit(TopicPartition topicPartition, int ledger) {
        try {
            partitionCounts.incrementAndGet();
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("Metrics on partition init listener failed, topic_partition={} ledger={}", topicPartition, ledger, t);
            }
        }
    }

    /**
     * Handles the destruction of a partition.
     * This method is invoked when a partition is destroyed, allowing for
     * custom behavior such as decrementing partition counters and removing
     * associated metrics from the global registry.
     *
     * @param topicPartition The {@link TopicPartition} that is being destroyed.
     * @param ledger The ledger ID associated with the topic partition.
     */
    @Override
    public void onPartitionDestroy(TopicPartition topicPartition, int ledger) {
        try {
            partitionCounts.decrementAndGet();
            Optional.ofNullable(topicReceiveCounters.remove(ledger)).ifPresent(Metrics.globalRegistry::remove);
            Optional.ofNullable(topicPushCounters.remove(ledger)).ifPresent(Metrics.globalRegistry::remove);
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("Metrics on partition destroy listener failed, topic_partition={} ledger={}", topicPartition, ledger, t);
            }
        }
    }

    /**
     * Handles the event when a partition gains a leader in a topic.
     *
     * @param topicPartition The TopicPartition instance representing the specific partition
     * within a topic that has gained a leader.
     */
    @Override
    public void onPartitionGetLeader(TopicPartition topicPartition) {
        partitionLeaderCounts.incrementAndGet();
    }

    /**
     * This method is triggered when a partition loses its leader.
     *
     * @param topicPartition the {@link TopicPartition} instance representing the partition that has lost its leader.
     */
    @Override
    public void onPartitionLostLeader(TopicPartition topicPartition) {
        partitionLeaderCounts.decrementAndGet();
    }

    /**
     * Invoked when a topic is created. This method logs the creation event
     * at the debug level if debug logging is enabled.
     *
     * @param topic the name of the topic that was created
     */
    @Override
    public void onTopicCreated(String topic) {
        if (logger.isDebugEnabled()) {
            logger.debug("The topic was created, and topic={}", topic);
        }
    }

    /**
     * Handles the event of a topic being deleted.
     *
     * @param topic the name of the topic that was deleted
     */
    @Override
    public void onTopicDeleted(String topic) {
        if (logger.isDebugEnabled()) {
            logger.debug("The topic was deleted, and topic={}", topic);
        }
    }

    /**
     * Callback method invoked when a partition assignment has changed.
     * This method will log a debug message indicating the old and new assignment values.
     *
     * @param topicPartition The partition within a topic whose assignment has changed.
     * @param oldAssigment The previous assignment configuration for the topic partition.
     * @param newAssigment The new assignment configuration for the topic partition.
     */
    @Override
    public void onPartitionChanged(TopicPartition topicPartition, TopicAssignment oldAssigment, TopicAssignment newAssigment) {
        if (logger.isDebugEnabled()) {
            logger.debug("The topic partition was changed, and the old={} new={}", oldAssigment, newAssigment);
        }
    }
}
