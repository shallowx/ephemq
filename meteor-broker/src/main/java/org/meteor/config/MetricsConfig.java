package org.meteor.config;

import java.util.Properties;

import static org.meteor.common.util.ObjectLiteralsTransformUtil.object2Int;

/**
 * Configuration class for metrics.
 * This class encapsulates the properties and provides access to metrics-related configurations.
 */
public class MetricsConfig {
    /**
     * This variable holds the key for the property that defines the sample limit for metrics collection.
     * It is used to retrieve the maximum number of metrics samples to be collected from the configuration properties.
     */
    private static final String METRICS_SAMPLE_LIMIT = "metrics.sample.limit";
    /**
     * Properties object that stores metrics configuration settings.
     */
    private final Properties prop;

    /**
     * Constructs a MetricsConfig object with the specified properties.
     *
     * @param prop the properties object containing configuration settings for metrics
     */
    public MetricsConfig(Properties prop) {
        this.prop = prop;
    }

    /**
     * Retrieves the configured limit for metrics sampling.
     * If the limit is not explicitly set in the properties, it defaults to 100.
     *
     * @return the metrics sample limit as an integer
     */
    public int getMetricsSampleLimit() {
        return object2Int(prop.getOrDefault(METRICS_SAMPLE_LIMIT, 100));
    }
}
