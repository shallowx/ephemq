package org.shallow.internal;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import org.shallow.ShutdownHook;
import org.shallow.common.logging.InternalLogger;
import org.shallow.common.logging.InternalLoggerFactory;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.mertics.MeterRegistrySetup;

import java.util.ServiceLoader;

public class ServerMetrics implements AutoCloseable{

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ServerMetrics.class);

    private final MeterRegistry registry = Metrics.globalRegistry;
    protected final ServiceLoader<MeterRegistrySetup> serviceLoader;
    private final BrokerConfig config;

    public ServerMetrics(ServiceLoader<MeterRegistrySetup> serviceLoader, BrokerConfig config) {
        this.serviceLoader = serviceLoader;
        this.config = config;
    }

    @Override
    public void close() throws Exception {

    }
}
