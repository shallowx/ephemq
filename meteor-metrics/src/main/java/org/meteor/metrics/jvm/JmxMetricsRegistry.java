package org.meteor.metrics.jvm;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;
import org.meteor.metrics.config.MetricsRegistrySetUp;

import java.util.Properties;

/**
 * Implementation of MetricsRegistrySetUp for JMX Meter Registry.
 * <p>
 * This class sets up a JMX Meter Registry using the Micrometer library. It adds the
 * JmxMeterRegistry to the global Metrics registry, enabling the collection and reporting
 * of metrics via JMX. The setUp method initializes the registry with a default configuration
 * and system clock.
 */
public class JmxMetricsRegistry implements MetricsRegistrySetUp {
    /**
     * Configures and initializes the JMX Meter Registry using the provided properties.
     *
     * @param properties Properties for configuring the JMX Meter Registry.
     *                   These properties may include configuration settings relevant
     *                   to the JMX Meter Registry or other metrics-related options.
     */
    @Override
    public void setUp(Properties properties) {
        JmxMeterRegistry jmxMeterRegistry = new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM);
        Metrics.addRegistry(jmxMeterRegistry);
    }

    /**
     * Shuts down the metrics registry.
     * <p>
     * This method is responsible for releasing any resources associated with
     * the metrics registry. In this implementation, the shutdown method performs
     * no actions.
     */
    @Override
    public void shutdown() {
        // do nothing
    }
}
