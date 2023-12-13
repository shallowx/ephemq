package org.meteor.configuration;

import org.meteor.common.util.TypeTransformUtils;

import java.util.Properties;

public class MetricsConfiguration {

    private static final String METRICS_SAMPLE_LIMIT = "metrics.sample.limit";
    private final Properties prop;

    public MetricsConfiguration(Properties prop) {
        this.prop = prop;
    }

    public int getMetricsSampleLimit() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(METRICS_SAMPLE_LIMIT, 100));
    }
}
