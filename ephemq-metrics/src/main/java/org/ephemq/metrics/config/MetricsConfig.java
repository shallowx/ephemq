package org.ephemq.metrics.config;

import static org.ephemq.common.util.ObjectLiteralsTransformUtil.object2Boolean;
import static org.ephemq.common.util.ObjectLiteralsTransformUtil.object2Int;
import static org.ephemq.common.util.ObjectLiteralsTransformUtil.object2String;
import java.util.Properties;

/**
 * Class responsible for managing configuration properties related to Prometheus metrics.
 * The settings include enabling/disabling metrics, the scrape URL, the exposed host address, and the port.
 */
public class MetricsConfig {
    /**
     * The property key for enabling or disabling Prometheus metrics.
     * When set to `true`, Prometheus metrics collection will be enabled.
     */
    private static final String METRICS_ENABLED = "metrics.prometheus.enable";
    /**
     * URL used by Prometheus to scrape metrics.
     * <p>
     * This constant holds the key used to retrieve the URL where Prometheus
     * can scrape metrics from the configuration properties.
     */
    private static final String METRICS_SCRAPE_URL = "metrics.prometheus.url";
    /**
     * Configuration property key for specifying the port on which Prometheus metrics will be exposed.
     */
    private static final String METRICS_PORT = "metrics.prometheus.exposed.port";
    /**
     * The address for exposing Prometheus metrics.
     * This configuration specifies the host address where the Prometheus metrics endpoint will be accessible.
     */
    private static final String METRICS_ADDRESS = "metrics.prometheus.exposed.host";
    /**
     * Stores configuration properties for the Prometheus metrics.
     * These properties define various settings such as enabling metrics, the scrape URL, host address, and port.
     */
    private final Properties props;

    /**
     * Constructs a MetricsConfig object with the specified properties.
     *
     * @param props the configuration properties to be used for setting up the metrics
     */
    private MetricsConfig(Properties props) {
        this.props = props;
    }

    /**
     * Creates a new instance of MetricsConfig using the provided properties.
     *
     * @param props Properties object containing configuration settings for metrics.
     * @return A new instance of MetricsConfig initialized with the specified properties.
     */
    public static MetricsConfig fromProps(Properties props) {
        return new MetricsConfig(props);
    }

    /**
     * Retrieves the scrape URL for Prometheus metrics.
     * If the URL is not configured, it defaults to "/prometheus".
     *
     * @return the configured scrape URL for Prometheus metrics
     */
    public String getMetricsScrapeUrl() {
        return object2String(props.getOrDefault(METRICS_SCRAPE_URL, "/prometheus"));
    }

    /**
     * Retrieves the configured metrics address for the Prometheus metrics endpoint.
     * If no address is specified in the properties, the default address "0.0.0.0" is returned.
     *
     * @return the metrics address as a string
     */
    public String getMetricsAddress() {
        return object2String(props.getOrDefault(METRICS_ADDRESS, "0.0.0.0"));
    }

    /**
     * Determines whether Prometheus metrics are enabled based on the configuration properties.
     *
     * @return true if metrics are enabled, false otherwise
     */
    public boolean isMetricsEnabled() {
        return object2Boolean(props.getOrDefault(METRICS_ENABLED, true));
    }

    /**
     * Retrieves the port number on which the Prometheus metrics should be exposed.
     * If the port is not explicitly set in the properties, a default value of 9128 is used.
     *
     * @return the port number for the Prometheus metrics
     */
    public int getMetricsPort() {
        return object2Int(props.getOrDefault(METRICS_PORT, 9128));
    }
}
