package org.meteor.proxy.core;

import java.util.Properties;
import org.meteor.config.CommonConfig;
import org.meteor.config.MetricsConfig;
import org.meteor.listener.MetricsListener;
import org.meteor.support.Manager;

public class ProxyMetricsListener extends MetricsListener {
    public ProxyMetricsListener(Properties properties, CommonConfig commonConfiguration,
                                MetricsConfig metricsConfiguration, Manager manager) {
        super(properties, commonConfiguration, metricsConfiguration, manager);
    }
}
