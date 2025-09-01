package org.ephemq.metrics.config;

import java.util.Properties;

/**
 * Interface for setting up and shutting down a metrics registry.
 * <p>
 * This interface defines the contract for initializing and releasing resources
 * associated with a metrics registry. Implementations of this interface are responsible
 * for configuring the metrics registry using provided properties and for properly
 * shutting down the registry to release any used resources.
 */
public interface MetricsRegistrySetUp {
    /**
     * Configures and initializes the metrics registry using the provided properties.
     *
     * @param properties Properties for configuring the metrics registry.
     *                   These properties may include configuration settings such as
     *                   thresholds, sampling rates, or other metrics-related options.
     */
    void setUp(Properties properties);

    /**
     * Shuts down the metrics registry.
     * <p>
     * This method is responsible for releasing any resources associated with
     * the metrics registry. It should be called to properly close and clean up
     * the registry when it is no longer needed.
     */
    void shutdown();
}
