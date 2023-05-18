package org.ostara.metrics;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.jmx.JmxConfig;

import java.util.Properties;

public class JmxMeterRegistry implements MeterRegistry {
    @Override
    public void setUp(Properties properties) {
        io.micrometer.jmx.JmxMeterRegistry jmxMeterRegistry = new io.micrometer.jmx.JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM);
        Metrics.addRegistry(jmxMeterRegistry);
    }

    @Override
    public void shutdown() {

    }
}
