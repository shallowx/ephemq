package org.leopard.internal.metrics;

import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.binder.jvm.*;
import io.micrometer.core.instrument.binder.system.FileDescriptorMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import org.leopard.common.logging.InternalLogger;
import org.leopard.common.logging.InternalLoggerFactory;
import org.leopard.internal.config.ServerConfig;
import org.leopard.ledger.Ledger;
import org.leopard.metrics.MeterRegistrySetup;
import org.leopard.metrics.NettyMetrics;

import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.leopard.internal.metrics.MetricsConstants.*;

@SuppressWarnings("all")
public class ServerMetrics implements LedgerMetricsListener, AutoCloseable {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ServerMetrics.class);

    private final MeterRegistry registry = Metrics.globalRegistry;
    protected final ServiceLoader<MeterRegistrySetup> serviceLoader;
    private final ServerConfig config;

    private final Map<Integer, Counter> topicReceiveCounters = new ConcurrentHashMap<>();

    private final AtomicInteger partitionCounts = new AtomicInteger();
    private final AtomicInteger partitionLeaderCounts = new AtomicInteger();

    public ServerMetrics(Properties properties, ServerConfig config) {
        this.config = config;
        this.serviceLoader = ServiceLoader.load(MeterRegistrySetup.class);
        for (MeterRegistrySetup meterRegistrySetup : serviceLoader) {
            meterRegistrySetup.setUp(properties);
        }

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
                            .register(Metrics.globalRegistry)
            );
        }
        counter.increment(count);
    }

    @Override
    public void onDispatchMessage(String topic, int ledger, int count) {

    }

    public void shutdown() {
        for (MeterRegistrySetup meterRegistrySetup : serviceLoader) {
            meterRegistrySetup.shutdown();
        }
    }
}
