package org.meteor.config;

import org.meteor.common.util.TypeTransformUtils;

import java.util.Properties;

public class MetricsConfig {

    private static final String METRICS_SAMPLE_LIMIT = "metrics.sample.limit";
    private final Properties prop;

    public MetricsConfig(Properties prop) {
        this.prop = prop;
    }

    public int getMetricsSampleLimit() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(METRICS_SAMPLE_LIMIT, 100));
    }
}
