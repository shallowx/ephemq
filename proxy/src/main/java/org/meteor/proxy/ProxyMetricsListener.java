package org.meteor.proxy;

import org.meteor.configuration.CommonConfiguration;
import org.meteor.configuration.MetricsConfiguration;
import org.meteor.configuration.ProxyConfiguration;
import org.meteor.listener.MetricsListener;
import org.meteor.management.Manager;

import java.util.Properties;

public class ProxyMetricsListener extends MetricsListener {
    public ProxyMetricsListener(Properties properties, CommonConfiguration commonConfiguration, MetricsConfiguration metricsConfiguration, Manager manager) {
        super(properties, commonConfiguration, metricsConfiguration, manager);
    }
}
