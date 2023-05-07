package org.ostara.metrics;

import static org.ostara.common.util.TypeUtils.object2Boolean;
import static org.ostara.common.util.TypeUtils.object2Int;
import static org.ostara.common.util.TypeUtils.object2String;
import java.util.Properties;

public class MetricsConfig {

    private final Properties props;

    private static final String METRICS_ENABLED = "metrics.prometheus.enable";
    private static final String METRICS_SCRAPE_URL = "metrics.prometheus.url";
    private static final String METRICS_PORT = "metrics.prometheus.exposed.port";
    private static final String METRICS_ADDRESS = "metrics.prometheus.exposed.host";

    private MetricsConfig(Properties props) {
        this.props = props;
    }

    public static MetricsConfig exchange(Properties props) {
        return new MetricsConfig(props);
    }

    public String getMetricsScrapeUrl() {
        return object2String(props.getOrDefault(METRICS_SCRAPE_URL, "/prometheus"));
    }

    public String getMetricsAddress() {
        return object2String(props.getOrDefault(METRICS_ADDRESS, "0.0.0.0"));
    }

    public boolean getMetricsEnabled() {
        return object2Boolean(props.getOrDefault(METRICS_ENABLED, true));
    }

    public int getMetricsPort() {
        return object2Int(props.getOrDefault(METRICS_PORT, 9090));
    }
}
