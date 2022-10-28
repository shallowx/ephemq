package org.shallow.mertics;

import org.shallow.common.util.TypeUtil;
import java.util.Properties;

public class MeterConfig {

    private final Properties config;

    private static final String METRICS_ENABLED = "metrics.enabled";
    private static final String METRICS_SCRAPE_URL = "metrics.scrape.url";
    private static final String METRICS_PORT = "metrics.port";
    private static final String METRICS_ADDRESS = "metrics.address";

    private MeterConfig(Properties config) {
        this.config = config;
    }

    public static MeterConfig exchange(Properties properties) {
        return new MeterConfig(properties);
    }

    public String getMetricsScrapeUrl() {
        return TypeUtil.object2String(config.getOrDefault(METRICS_SCRAPE_URL, "/prometheus"));
    }

    public String getMetricsAddress() {
        return TypeUtil.object2String(config.getOrDefault(METRICS_ADDRESS, "0.0.0.0"));
    }

    public boolean getMetricsEnabled() {
        return TypeUtil.object2Boolean(config.getOrDefault(METRICS_ENABLED, true));
    }

    public int getMetricsPort() {
        return TypeUtil.object2Int(config.getOrDefault(METRICS_PORT, 9000));
    }
}
