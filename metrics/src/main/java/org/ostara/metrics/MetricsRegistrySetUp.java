package org.ostara.metrics;

import java.util.Properties;

public interface MetricsRegistrySetUp {
    void setUp(Properties properties);

    void shutdown();
}
