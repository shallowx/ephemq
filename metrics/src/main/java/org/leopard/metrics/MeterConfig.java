package org.leopard.metrics;

import java.util.Properties;

import static org.leopard.common.util.TypeUtils.*;

public class MeterConfig {

    private final Properties props;

    private static final String METRICS_ENABLED = "metrics.enabled";
    private static final String METRICS_SCRAPE_URL = "metrics.scrape.url";
    private static final String METRICS_PORT = "metrics.port";
    private static final String METRICS_ADDRESS = "metrics.address";

    private MeterConfig(Properties props) {
        this.props = props;
    }

    public static MeterConfig exchange(Properties props) {
        return new MeterConfig(props);
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
        return object2Int(props.getOrDefault(METRICS_PORT, 9000));
    }
}
