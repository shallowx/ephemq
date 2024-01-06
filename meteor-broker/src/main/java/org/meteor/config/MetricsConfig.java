package org.meteor.config;

import java.util.Properties;

import static org.meteor.common.util.ObjectLiteralsTransformUtil.object2Int;

public class MetricsConfig {
    private static final String METRICS_SAMPLE_LIMIT = "metrics.sample.limit";
    private final Properties prop;

    public MetricsConfig(Properties prop) {
        this.prop = prop;
    }

    public int getMetricsSampleLimit() {
        return object2Int(prop.getOrDefault(METRICS_SAMPLE_LIMIT, 100));
    }
}
