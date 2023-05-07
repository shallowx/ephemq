package org.ostara.internal.metrics;

import static org.ostara.internal.metrics.MetricsConstants.BROKER_TAG;
import static org.ostara.internal.metrics.MetricsConstants.CLUSTER_TAG;
import static org.ostara.internal.metrics.MetricsConstants.LEDGER_TAG;
import static org.ostara.internal.metrics.MetricsConstants.QUEUE_TAG;
import static org.ostara.internal.metrics.MetricsConstants.TOPIC_PARTITION_COUNTER_GAUGE_NAME;
import static org.ostara.internal.metrics.MetricsConstants.TOPIC_PARTITION_LEADER_COUNTER_GAUGE_NAME;
import static org.ostara.internal.metrics.MetricsConstants.TOPIC_TAG;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmInfoMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.FileDescriptorMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.internal.config.ServerConfig;
import org.ostara.ledger.Ledger;
import org.ostara.metrics.MeterRegistrySetup;
import org.ostara.metrics.NettyMetrics;
import org.ostara.metrics.PrometheusRegistrySetup;

@SuppressWarnings("all")
public class ServerMetrics implements LedgerMetricsListener, ApiListener, AutoCloseable {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ServerMetrics.class);

    private final MeterRegistry registry = Metrics.globalRegistry;
    protected final ServiceLoader<MeterRegistrySetup> serviceLoader;
    private final ServerConfig config;

    private final Map<Integer, Counter> topicReceiveCounters = new ConcurrentHashMap<>();

    private final AtomicInteger partitionCounts = new AtomicInteger();
    private final AtomicInteger partitionLeaderCounts = new AtomicInteger();
    private final int metricsSampleCount;

    public ServerMetrics(Properties properties, ServerConfig config) {
        this.config = config;
        this.metricsSampleCount = config.getMetricsSampleCount();
        this.serviceLoader = ServiceLoader.load(MeterRegistrySetup.class);
        MeterRegistrySetup meterRegistrySetup = new PrometheusRegistrySetup();
        meterRegistrySetup.setUp(properties);

        Tags tags = Tags.of(CLUSTER_TAG, config.getClusterName()).and(BROKER_TAG, config.getClusterName());

        new UptimeMetrics(tags).bindTo(registry);
        new FileDescriptorMetrics(tags).bindTo(registry);
        new ClassLoaderMetrics(tags).bindTo(registry);
        new JvmMemoryMetrics(tags).bindTo(registry);
        new JvmGcMetrics(tags).bindTo(registry);
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
    public void close() throws Exception {

    }

    @Override
    public void onInitLedger(Ledger ledger) {

    }

    @Override
    public void onCommand(int code, int bytes, long cost, boolean ret) {

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
                    Counter.builder(MetricsConstants.TOPIC_MESSAGE_RECEIVE_COUNTER)
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

    public void shutdown() {
        for (MeterRegistrySetup meterRegistrySetup : serviceLoader) {
            meterRegistrySetup.shutdown();
        }
    }
}
