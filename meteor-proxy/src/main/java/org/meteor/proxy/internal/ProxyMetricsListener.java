package org.meteor.proxy.internal;

import org.meteor.config.CommonConfig;
import org.meteor.config.MetricsConfig;
import org.meteor.coordinator.Coordinator;
import org.meteor.listener.MetricsListener;

import java.util.Properties;

public class ProxyMetricsListener extends MetricsListener {
    public ProxyMetricsListener(Properties properties, CommonConfig commonConfiguration, MetricsConfig metricsConfiguration, Coordinator coordinator) {
        super(properties, commonConfiguration, metricsConfiguration, coordinator);
    }
}
