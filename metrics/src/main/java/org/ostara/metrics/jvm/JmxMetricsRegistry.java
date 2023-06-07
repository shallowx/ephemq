package org.ostara.metrics.jvm;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;
import org.ostara.metrics.MetricsRegistrySetUp;

import java.util.Properties;

public class JmxMetricsRegistry implements MetricsRegistrySetUp {
    @Override
    public void setUp(Properties properties) {
        JmxMeterRegistry jmxMeterRegistry = new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM);
        Metrics.addRegistry(jmxMeterRegistry);
    }

    @Override
    public void shutdown() {
        // do nothing
    }
}
