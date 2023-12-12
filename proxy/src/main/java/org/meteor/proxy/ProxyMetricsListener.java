package org.meteor.proxy;

import org.meteor.core.CoreConfig;
import org.meteor.listener.MetricsListener;
import org.meteor.management.Manager;

import java.util.Properties;

public class ProxyMetricsListener extends MetricsListener {

    public ProxyMetricsListener(Properties properties, CoreConfig config, Manager manager) {
        super(properties, config, manager);
    }
}
