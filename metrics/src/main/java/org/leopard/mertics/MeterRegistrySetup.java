package org.leopard.mertics;

import java.util.Properties;

public interface MeterRegistrySetup {
    void setUp(Properties properties);
    void shutdown();
}
