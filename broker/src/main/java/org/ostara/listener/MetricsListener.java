package org.ostara.listener;

import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.binder.jvm.*;
import io.micrometer.core.instrument.binder.system.FileDescriptorMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import io.netty.util.internal.StringUtil;
import org.ostara.common.Node;
import org.ostara.common.TopicAssignment;
import org.ostara.common.TopicPartition;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.core.CoreConfig;
import org.ostara.log.Log;
import org.ostara.management.Manager;
import org.ostara.metrics.*;
import org.ostara.metrics.jvm.DefaultJVMInfoMetrics;
import org.ostara.metrics.jvm.JmxMetricsRegistry;
import org.ostara.metrics.netty.NettyMetrics;

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
    private final CoreConfig config;
    private final Manager manager;
    private final Map<Integer, Counter> topicReceiveCounters = new ConcurrentHashMap<>();
    private final Map<Integer, Counter> topicPushCounters = new ConcurrentHashMap<>();
    private final Map<Integer, Counter> requestSuccesses = new ConcurrentHashMap<>();
    private final Map<Integer, Counter> requestFailures = new ConcurrentHashMap<>();
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

    public MetricsListener(Properties properties, CoreConfig config, Manager manager) {
        this.config = config;
        this.manager = manager;
        this.metricsSample = config.getMetricsSampleCounts();
        this.meterRegistrySetup = new PrometheusRegistry();
        this.meterRegistrySetup.setUp(properties);

        MetricsRegistrySetUp jmxMeterRegistrySetup = new JmxMetricsRegistry();
        jmxMeterRegistrySetup.setUp(properties);



        Tags tags = Tags.of(CLUSTER_TAG, config.getClusterName()).and(BROKER_TAG, config.getClusterName());
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
    @Override
    public void close() throws Exception {
        try {
            this.jvmGcMetrics.close();
            this.meterRegistrySetup.shutdown();
        } catch (Throwable t){
            logger.error(t.getMessage(), t);
        }
    }

    @Override
    public void onCommand(int code, int bytes, long cost, boolean ret) {
        try {
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

            counter.increment();
            Integer sample = metricsSampleCount.get();
            metricsSampleCount.set(sample + 1);
            if (sample > metricsSample) {
                metricsSampleCount.set(0);
                DistributionSummary drs = requestSizeSummary.computeIfAbsent(code,
                        s -> DistributionSummary.builder(REQUEST_SIZE_SUMMARY_NAME)
                                .tags(Tags.of(TYPE_TAG, String.valueOf(code))
                                        .and(BROKER_TAG, config.getServerId())
                                        .and(CLUSTER_TAG, config.getClusterName()))
                                .baseUnit("bytes")
                                .distributionStatisticExpiry(Duration.ofSeconds(30))
                                .publishPercentiles(0.99, 0.999, 0.9)
                                .register(registry)
                );
                drs.record(bytes);

                DistributionSummary drt = requestTimesSummary.computeIfAbsent(code,
                        s -> DistributionSummary.builder(API_RESPONSE_TIME_NAME)
                                .tags(Tags.of(TYPE_TAG, String.valueOf(code))
                                        .and(BROKER_TAG, config.getServerId())
                                        .and(CLUSTER_TAG, config.getClusterName()))
                                .baseUnit("ns")
                                .distributionStatisticExpiry(Duration.ofSeconds(30))
                                .publishPercentiles(0.99, 0.999, 0.9)
                                .register(registry)
                );
                drt.record(bytes);
            }
        } catch (Throwable t) {
            logger.error("Metrics on command listener failed, code={}", code, t);
        }
    }

    @Override
    public void onInitLog(Log log) {

    }

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
            logger.error("Metrics on receive message listener failed, topic={} ledger={} count={}", topic, ledger, count, t);
        }
    }

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
        } catch (Throwable t){
            logger.error("Metrics on sync message listener failed, topic={} ledger={} count={}", topic, ledger, count, t);
        }
    }

    @Override
    public void onPushMessage(String topic, int ledger, int count) {
        try {
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
        } catch (Throwable t){
            logger.error("Metrics on push message listener failed, topic={} ledger={} count={}", topic, ledger, count, t);
        }
    }

    @Override
    public void onChunkPushMessage(String topic, int ledger, int count) {

    }

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
            } catch (Throwable t){
                logger.error("Storage metrics started failed, cluster={} server_id={} executor={}", clusterName, serverId, executor.toString(), t);
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
            } catch (Throwable t){
                logger.error("Dispatch metrics started failed, cluster={} server_id={} executor={}", clusterName, serverId, executor.toString(), t);
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
            } catch (Throwable t){
                logger.error("Command metrics started failed, cluster={} server_id={} executor={}", clusterName, serverId, executor.toString(), t);
            }
        }
    }

    @Override
    public void onShutdown(Node node) {

    }

    @Override
    public void onPartitionInit(TopicPartition topicPartition, int ledger) {
        try {
            partitionCounts.incrementAndGet();
        }catch (Throwable t){
            logger.error("Metrics on partition init listener failed, topicPartition={} ledger={}", topicPartition, ledger, t);
        }
    }

    @Override
    public void onPartitionDestroy(TopicPartition topicPartition, int ledger) {
        try {
            partitionCounts.decrementAndGet();
            Optional.ofNullable(topicReceiveCounters.remove(ledger)).ifPresent(Metrics.globalRegistry::remove);
            Optional.ofNullable(topicPushCounters.remove(ledger)).ifPresent(Metrics.globalRegistry::remove);
        }catch (Throwable t){
            logger.error("Metrics on partition destroy listener failed, topicPartition={} ledger={}", topicPartition, ledger, t);
        }
    }

    @Override
    public void onPartitionGetLeader(TopicPartition topicPartition) {
        partitionLeaderCounts.incrementAndGet();
    }

    @Override
    public void onPartitionLostLeader(TopicPartition topicPartition) {
        partitionLeaderCounts.decrementAndGet();
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
