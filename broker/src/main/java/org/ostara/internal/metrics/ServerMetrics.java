package org.ostara.internal.metrics;

import static org.ostara.internal.metrics.MetricsConstants.BROKER_TAG;
import static org.ostara.internal.metrics.MetricsConstants.CLUSTER_TAG;
import static org.ostara.internal.metrics.MetricsConstants.LEDGER_TAG;
import static org.ostara.internal.metrics.MetricsConstants.QUEUE_TAG;
import static org.ostara.internal.metrics.MetricsConstants.TOPIC_PARTITION_COUNTER_GAUGE_NAME;
import static org.ostara.internal.metrics.MetricsConstants.TOPIC_PARTITION_LEADER_COUNTER_GAUGE_NAME;
import static org.ostara.internal.metrics.MetricsConstants.TOPIC_TAG;

import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.binder.jvm.*;
import io.micrometer.core.instrument.binder.system.FileDescriptorMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import io.netty.util.internal.StringUtil;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.common.metadata.Node;
import org.ostara.internal.config.ServerConfig;
import org.ostara.ledger.Ledger;
import org.ostara.metrics.JmxMeterRegistrySetup;
import org.ostara.metrics.MeterRegistrySetup;
import org.ostara.metrics.NettyMetrics;
import org.ostara.metrics.PrometheusRegistrySetup;
import org.ostara.remote.util.NetworkUtils;

@SuppressWarnings("all")
public class ServerMetrics implements LedgerMetricsListener, ApiListener, AutoCloseable {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ServerMetrics.class);

    private final MeterRegistry registry = Metrics.globalRegistry;
    private final ServerConfig config;

    private final Map<Integer, Counter> topicReceiveCounters = new ConcurrentHashMap<>();
    private final Map<Integer, Counter> requestSuccessed = new ConcurrentHashMap<>();
    private final Map<Integer, Counter> requestFailured = new ConcurrentHashMap<>();
    private final Map<Integer, DistributionSummary> requestSizeSummary= new ConcurrentHashMap<>();
    private final Map<Integer, DistributionSummary> requestTimesSummary= new ConcurrentHashMap<>();
    private final AtomicInteger partitionCounts = new AtomicInteger();
    private final AtomicInteger partitionLeaderCounts = new AtomicInteger();
    private final int metricsSample;
    private final MeterRegistrySetup meterRegistrySetup;
    private final JvmGcMetrics jvmGcMetrics;

    private final FastThreadLocal<Integer> metricsSampleCount = new FastThreadLocal<>(){
        @Override
        protected Integer initialValue() throws Exception {
            return 0;
        }
    };

    public ServerMetrics(Properties properties, ServerConfig config) {
        this.config = config;
        this.metricsSample = config.getMetricsSampleCount();
        this.meterRegistrySetup = new PrometheusRegistrySetup();
        this.meterRegistrySetup.setUp(properties);

        JmxMeterRegistrySetup jmxMeterRegistrySetup = new JmxMeterRegistrySetup();
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

        Gauge.builder(TOPIC_PARTITION_COUNTER_GAUGE_NAME, partitionCounts, AtomicInteger::doubleValue)
                .tags(Tags.of(CLUSTER_TAG, config.getClusterName()).and(BROKER_TAG, config.getServerId()))
                .register(registry);

        Gauge.builder(TOPIC_PARTITION_LEADER_COUNTER_GAUGE_NAME, partitionLeaderCounts, AtomicInteger::doubleValue)
                .tags(Tags.of(CLUSTER_TAG, config.getClusterName()).and(BROKER_TAG, config.getServerId()))
                .register(registry);
    }

    @Override
    public void startUp(Node node) {
        // storage metrics
        EventExecutorGroup storageSventExecutors = NetworkUtils.newEventExecutorGroup(Runtime.getRuntime().availableProcessors(), "storage-metrics");
        for (EventExecutor executor : storageSventExecutors) {
            SingleThreadEventExecutor se = (SingleThreadEventExecutor) executor;
            Gauge.builder("netty-pending-task", se, SingleThreadEventExecutor::pendingTasks)
                    .tag(CLUSTER_TAG, config.getClusterName())
                    .tag(BROKER_TAG, config.getServerId())
                    .tag("type", "storage")
                    .tag("name", StringUtil.EMPTY_STRING)
                    .tag("id", se.threadProperties().name()).register(registry);
        }

        // dispatch metrics
        EventExecutorGroup dispatchEventExecutors = NetworkUtils.newEventExecutorGroup(Runtime.getRuntime().availableProcessors(), "dispatch-metrics");
        for (EventExecutor executor : dispatchEventExecutors) {
            SingleThreadEventExecutor se = (SingleThreadEventExecutor) executor;
            Gauge.builder("netty-pending-task", se, SingleThreadEventExecutor::pendingTasks)
                    .tag(CLUSTER_TAG, config.getClusterName())
                    .tag(BROKER_TAG, config.getServerId())
                    .tag("type", "dispathc")
                    .tag("name", StringUtil.EMPTY_STRING)
                    .tag("id", se.threadProperties().name()).register(registry);
        }

        // command metrics
        EventExecutorGroup commandEventExecutors = NetworkUtils.newEventExecutorGroup(Runtime.getRuntime().availableProcessors(), "dispatch-metrics");
        for (EventExecutor executor : commandEventExecutors) {
            SingleThreadEventExecutor se = (SingleThreadEventExecutor) executor;
            Gauge.builder("netty-pending-task", se, SingleThreadEventExecutor::pendingTasks)
                    .tag(CLUSTER_TAG, config.getClusterName())
                    .tag(BROKER_TAG, config.getServerId())
                    .tag("type", "command")
                    .tag("name", StringUtil.EMPTY_STRING)
                    .tag("id", se.threadProperties().name()).register(registry);
        }
    }

    @Override
    public void close() throws Exception {
        this.meterRegistrySetup.shutdown();
    }

    @Override
    public void onInitLedger(Ledger ledger) {

    }

    @Override
    public void onCommand(int code, int bytes, long cost, boolean ret) {
        Map<Integer, Counter> counters = ret ? requestSuccessed : requestFailured;
        int type = code;
        Counter counter = counters.get(type);
        if (counter == null) {
            counter = counters.computeIfAbsent(type,
                    s -> Counter.builder("request_state_count")
                    .tags(Tags.of("request_type", String.valueOf(type))
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
            DistributionSummary drs = requestSizeSummary.computeIfAbsent(type,
                    s -> DistributionSummary.builder("request_size")
                            .tags(Tags.of("request_type", String.valueOf(type))
                                    .and(BROKER_TAG, config.getServerId())
                                    .and(CLUSTER_TAG, config.getClusterName()))
                            .baseUnit("bytes")
                            .distributionStatisticExpiry(Duration.ofSeconds(30))
                            .publishPercentiles(0.99, 0.999, 0.9)
                            .register(registry));
            drs.record(bytes);

            DistributionSummary drt = requestTimesSummary.computeIfAbsent(type,
                    s -> DistributionSummary.builder("api_response_time")
                            .tags(Tags.of("request_type", String.valueOf(type))
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
    public void onReceiveMessage(String topic, String queue, int ledger, int count) {
        Counter counter = topicReceiveCounters.get(ledger);
        if (counter == null) {
            counter = topicReceiveCounters.computeIfAbsent(ledger, metrics ->
                    Counter.builder(MetricsConstants.TOPIC_MESSAGE_RECEIVE_COUNTER)
                            .tag(TOPIC_TAG, topic)
                            .tag(LEDGER_TAG, String.valueOf(ledger))
                            .tag(QUEUE_TAG, queue)
                            .tag(CLUSTER_TAG, config.getClusterName())
                            .tag(BROKER_TAG, config.getServerId())
                            .tag("type", "send")
                            .register(Metrics.globalRegistry)
            );
        }
        counter.increment(count);
    }

    @Override
    public void onDispatchMessage(String topic, int ledger, int count) {
        Counter counter = topicReceiveCounters.get(ledger);
        if (counter == null) {
            counter = topicReceiveCounters.computeIfAbsent(ledger, metrics ->
                    Counter.builder(MetricsConstants.TOPIC_MESSAGE_PUSH_COUNTER)
                            .tag(TOPIC_TAG, topic)
                            .tag(LEDGER_TAG, String.valueOf(ledger))
                            .tag(CLUSTER_TAG, config.getClusterName())
                            .tag(BROKER_TAG, config.getServerId())
                            .tag("type", "single")
                            .register(Metrics.globalRegistry)
            );
        }
        counter.increment(count);
    }

    @Override
    public void onPartitionInit() {
        try {
            partitionCounts.incrementAndGet();
        }catch (Throwable ignord){}
    }

    @Override
    public void onPartitionDestroy() {
        try {
            partitionCounts.decrementAndGet();
        }catch (Throwable ignord){}
    }
}
