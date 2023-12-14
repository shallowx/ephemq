package org.meteor.proxy.internal;

import org.meteor.configuration.CommonConfiguration;
import org.meteor.configuration.MetricsConfiguration;
import org.meteor.listener.MetricsListener;
import org.meteor.coordinatior.Coordinator;

import java.util.Properties;

public class ProxyMetricsListener extends MetricsListener {
    public ProxyMetricsListener(Properties properties, CommonConfiguration commonConfiguration, MetricsConfiguration metricsConfiguration, Coordinator coordinator) {
        super(properties, commonConfiguration, metricsConfiguration, coordinator);
    }
}
