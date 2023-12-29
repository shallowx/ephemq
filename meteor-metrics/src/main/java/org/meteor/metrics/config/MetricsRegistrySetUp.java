package org.meteor.metrics.config;

import java.util.Properties;

public interface MetricsRegistrySetUp {
    void setUp(Properties properties);
    void shutdown();
}
