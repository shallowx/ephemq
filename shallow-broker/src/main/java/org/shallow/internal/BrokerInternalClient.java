package org.shallow.internal;

import org.shallow.ClientConfig;
import org.shallow.pool.ShallowChannelHealthChecker;

public class BrokerInternalClient extends AbstractInternalClient{

    private final BrokerManager brokerManager;

    public BrokerInternalClient(String name, ClientConfig config, BrokerManager brokerManager) {
        super(name, config);
        this.brokerManager = brokerManager;
    }

    public BrokerInternalClient(String name, ClientConfig config, BrokerManager brokerManager,  ShallowChannelHealthChecker healthChecker) {
        super(name, config, healthChecker);
        this.brokerManager = brokerManager;
    }
}
