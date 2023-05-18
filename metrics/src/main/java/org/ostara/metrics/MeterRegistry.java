package org.ostara.metrics;

import java.util.Properties;

public interface MeterRegistry {
    void setUp(Properties properties);

    void shutdown();
}
