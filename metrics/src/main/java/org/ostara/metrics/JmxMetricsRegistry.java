package org.ostara.metrics;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;

import java.util.Properties;

public class JmxMetricsRegistry implements MetricsRegistry {
    @Override
    public void setUp(Properties properties) {
        JmxMeterRegistry jmxMeterRegistry = new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM);
        Metrics.addRegistry(jmxMeterRegistry);
    }

    @Override
    public void shutdown() {

    }
}
