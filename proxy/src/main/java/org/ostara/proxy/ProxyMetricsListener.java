package org.ostara.proxy;

import org.ostara.core.CoreConfig;
import org.ostara.listener.MetricsListener;
import org.ostara.management.Manager;

import java.util.Properties;

public class ProxyMetricsListener extends MetricsListener {

    public ProxyMetricsListener(Properties properties, CoreConfig config, Manager manager) {
        super(properties, config, manager);
    }
}
