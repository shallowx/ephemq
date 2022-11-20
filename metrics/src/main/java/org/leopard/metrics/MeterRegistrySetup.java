package org.leopard.metrics;

import java.util.Properties;

public interface MeterRegistrySetup {
    void setUp(Properties properties);

    void shutdown();
}
