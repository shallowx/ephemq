package org.ostara.metrics;

import java.util.Properties;

public interface MetricsRegistry {
    void setUp(Properties properties);

    void shutdown();
}
