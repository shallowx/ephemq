package org.meteor.proxy.core;

import java.util.Properties;
import org.meteor.config.CommonConfig;
import org.meteor.config.MetricsConfig;
import org.meteor.listener.MetricsListener;
import org.meteor.support.Manager;

/**
 * This class extends MetricsListener to provide additional functionalities
 * for proxy metrics. It initializes with properties, common configuration,
 * metrics configuration, and a manager.
 */
public class ProxyMetricsListener extends MetricsListener {
    /**
     * Constructs a ProxyMetricsListener with the specified configurations.
     *
     * @param properties           the properties object containing configuration settings
     * @param commonConfiguration  the common configuration object providing server settings
     * @param metricsConfiguration the metrics configuration object
     * @param manager              the manager responsible for handling various managerial tasks
     */
    public ProxyMetricsListener(Properties properties, CommonConfig commonConfiguration,
                                MetricsConfig metricsConfiguration, Manager manager) {
        super(properties, commonConfiguration, metricsConfiguration, manager);
    }
}
