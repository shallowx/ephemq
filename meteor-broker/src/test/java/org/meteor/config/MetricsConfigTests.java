package org.meteor.config;

import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.Properties;

public class MetricsConfigTests {

    @Test
    public void testMetricsConfig() {
        Properties prop = new Properties();
        prop.put("metrics.sample.limit", 10000);

        ServerConfig serverConfig = new ServerConfig(prop);
        MetricsConfig metricsConfig = serverConfig.getMetricsConfig();
        Assertions.assertNotNull(metricsConfig);
        Assertions.assertEquals(10000, metricsConfig.getMetricsSampleLimit());
    }
}
