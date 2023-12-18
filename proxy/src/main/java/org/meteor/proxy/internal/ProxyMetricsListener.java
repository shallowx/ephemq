package org.meteor.proxy.internal;

import org.meteor.configuration.CommonConfig;
import org.meteor.configuration.MetricsConfig;
import org.meteor.listener.MetricsListener;
import org.meteor.coordinatior.Coordinator;

import java.util.Properties;

public class ProxyMetricsListener extends MetricsListener {
    public ProxyMetricsListener(Properties properties, CommonConfig commonConfiguration, MetricsConfig metricsConfiguration, Coordinator coordinator) {
        super(properties, commonConfiguration, metricsConfiguration, coordinator);
    }
}
