package org.ostara.listener;

import io.micrometer.core.instrument.*;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import io.netty.util.internal.StringUtil;
import org.ostara.management.Manager;
import org.ostara.metrics.JmxMetricsRegistry;
import io.micrometer.core.instrument.binder.jvm.*;
import io.micrometer.core.instrument.binder.system.FileDescriptorMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import io.netty.util.concurrent.FastThreadLocal;
import org.ostara.common.Node;
import org.ostara.common.TopicAssignment;
import org.ostara.common.TopicPartition;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.core.Config;
import org.ostara.log.Log;
import org.ostara.metrics.MetricsRegistrySetUp;
import org.ostara.metrics.NettyMetrics;
import org.ostara.metrics.PrometheusRegistry;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import static org.ostara.metrics.MetricsConstants.*;

public class MetricsListener implements APIListener, ServerListener, LogListener, TopicListener, AutoCloseable{

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MetricsListener.class);
    private final io.micrometer.core.instrument.MeterRegistry registry = Metrics.globalRegistry;
    private final Config config;
    private Manager manager;
    private final Map<Integer, Counter> topicReceiveCounters = new ConcurrentHashMap<>();
    private final Map<Integer, Counter> topicPushCounters = new ConcurrentHashMap<>();
    private final Map<Integer, Counter> requestSuccessed = new ConcurrentHashMap<>();
    private final Map<Integer, Counter> requestFailured = new ConcurrentHashMap<>();
    private final Map<Integer, DistributionSummary> requestSizeSummary= new ConcurrentHashMap<>();
    private final Map<Integer, DistributionSummary> requestTimesSummary= new ConcurrentHashMap<>();
    private final AtomicInteger partitionCounts = new AtomicInteger();
    private final AtomicInteger partitionLeaderCounts = new AtomicInteger();
    private final int metricsSample;
    private final MetricsRegistrySetUp meterRegistrySetup;
    private final JvmGcMetrics jvmGcMetrics;

    private final FastThreadLocal<Integer> metricsSampleCount = new FastThreadLocal<>(){
        @Override
        protected Integer initialValue() throws Exception {
            return 0;
        }
    };

    public MetricsListener(Properties properties, Config config, Manager manager) {
        this.config = config;
        this.manager = manager;
        this.metricsSample = config.getMetricsSampleCounts();
        this.meterRegistrySetup = new PrometheusRegistry();
        this.meterRegistrySetup.setUp(properties);

        MetricsRegistrySetUp jmxMeterRegistrySetup = new JmxMetricsRegistry();
        jmxMeterRegistrySetup.setUp(properties);

        Tags tags = Tags.of(CLUSTER_TAG, config.getClusterName()).and(BROKER_TAG, config.getClusterName());

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
    @Override
    public void close() throws Exception {
        this.meterRegistrySetup.shutdown();
    }

    @Override
    public void onCommand(int code, int bytes, long cost, boolean ret) {
        Map<Integer, Counter> counters = ret ? requestSuccessed : requestFailured;
        Counter counter = counters.get(code);
        if (counter == null) {
            counter = counters.computeIfAbsent(code,
                    s -> Counter.builder("request_state_count")
                            .tags(Tags.of("request_type", String.valueOf(code))
                                    .and(BROKER_TAG, config.getServerId())
                                    .and(CLUSTER_TAG, config.getClusterName())
                                    .and("ret", ret ? "success" : "failure"))
                            .register(registry));
        }

        counter.increment();
        Integer sample = metricsSampleCount.get();
        metricsSampleCount.set(sample + 1);
        if (sample > metricsSample) {
            metricsSampleCount.set(0);
            DistributionSummary drs = requestSizeSummary.computeIfAbsent(code,
                    s -> DistributionSummary.builder("request_size")
                            .tags(Tags.of("request_type", String.valueOf(code))
                                    .and(BROKER_TAG, config.getServerId())
                                    .and(CLUSTER_TAG, config.getClusterName()))
                            .baseUnit("bytes")
                            .distributionStatisticExpiry(Duration.ofSeconds(30))
                            .publishPercentiles(0.99, 0.999, 0.9)
                            .register(registry));
            drs.record(bytes);

            DistributionSummary drt = requestTimesSummary.computeIfAbsent(code,
                    s -> DistributionSummary.builder("api_response_time")
                            .tags(Tags.of("request_type", String.valueOf(code))
                                    .and(BROKER_TAG, config.getServerId())
                                    .and(CLUSTER_TAG, config.getClusterName()))
                            .baseUnit("ns")
                            .distributionStatisticExpiry(Duration.ofSeconds(30))
                            .publishPercentiles(0.99, 0.999, 0.9)
                            .register(registry));
            drt.record(bytes);
        }
    }

    @Override
    public void onInitLog(Log log) {

    }

    @Override
    public void onReceiveMessage(String topic, int ledger, int count) {
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
    }

    @Override
    public void onSyncMessage(String topic, int ledger, int count) {
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
    }

    @Override
    public void onPushMessage(String topic, int ledger, int count) {
        Counter counter = topicPushCounters.get(ledger);
        if(counter == null) {
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
    }

    @Override
    public void onChunkPushMessage(String topic, int ledger, int count) {

    }

    @Override
    public void onStartup(Node node) {
        // storage metrics
        for (EventExecutor executor : manager.getMessageStorageEventExecutorGroup()) {
            SingleThreadEventExecutor se = (SingleThreadEventExecutor) executor;
            Gauge.builder("netty-pending-task", se, SingleThreadEventExecutor::pendingTasks)
                    .tag(CLUSTER_TAG, config.getClusterName())
                    .tag(BROKER_TAG, config.getServerId())
                    .tag(TYPE_TAG, "storage")
                    .tag("name", StringUtil.EMPTY_STRING)
                    .tag("id", se.threadProperties().name()).register(registry);
        }

        // dispatch metrics
        for (EventExecutor executor : manager.getMessageDispatchEventExecutorGroup()) {
            SingleThreadEventExecutor se = (SingleThreadEventExecutor) executor;
            Gauge.builder("netty-pending-task", se, SingleThreadEventExecutor::pendingTasks)
                    .tag(CLUSTER_TAG, config.getClusterName())
                    .tag(BROKER_TAG, config.getServerId())
                    .tag(TYPE_TAG, "dispatch")
                    .tag("name", StringUtil.EMPTY_STRING)
                    .tag("id", se.threadProperties().name()).register(registry);
        }

        // command metrics
        for (EventExecutor executor : manager.getCommandHandleEventExecutorGroup()) {
            SingleThreadEventExecutor se = (SingleThreadEventExecutor) executor;
            Gauge.builder("netty-pending-task", se, SingleThreadEventExecutor::pendingTasks)
                    .tag(CLUSTER_TAG, config.getClusterName())
                    .tag(BROKER_TAG, config.getServerId())
                    .tag(TYPE_TAG, "command")
                    .tag("name", StringUtil.EMPTY_STRING)
                    .tag("id", se.threadProperties().name()).register(registry);
        }
    }

    @Override
    public void onShutdown(Node node) {

    }

    @Override
    public void onPartitionInit(TopicPartition topicPartition, int ledger) {
        try {
            partitionCounts.incrementAndGet();
        }catch (Throwable ignored){}
    }

    @Override
    public void onPartitionDestroy(TopicPartition topicPartition, int ledger) {
        try {
            partitionCounts.decrementAndGet();
            Optional.ofNullable(topicReceiveCounters.remove(ledger)).ifPresent(Metrics.globalRegistry::remove);
            Optional.ofNullable(topicPushCounters.remove(ledger)).ifPresent(Metrics.globalRegistry::remove);
        }catch (Throwable ignored){}
    }

    @Override
    public void onPartitionGetLeader(TopicPartition topicPartition) {

    }

    @Override
    public void onPartitionLostLeader(TopicPartition topicPartition) {

    }

    @Override
    public void onTopicCreated(String topic) {

    }

    @Override
    public void onTopicDeleted(String topic) {

    }

    @Override
    public void onPartitionChanged(TopicPartition topicPartition, TopicAssignment oldAssigment, TopicAssignment newAssigment) {

    }
}
