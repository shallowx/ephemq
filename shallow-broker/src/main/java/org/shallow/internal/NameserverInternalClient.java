package org.shallow.internal;

import org.shallow.ClientConfig;
import org.shallow.pool.ShallowChannelHealthChecker;

public class NameserverInternalClient extends AbstractInternalClient  {

    private final BrokerManager brokerManager;

    public NameserverInternalClient(String name, ClientConfig config,BrokerManager brokerManager) {
        super(name, config);
        this.brokerManager = brokerManager;
    }

    public NameserverInternalClient(String name, ClientConfig config, ShallowChannelHealthChecker healthChecker, BrokerManager brokerManager) {
        super(name, config, healthChecker);
        this.brokerManager = brokerManager;
    }
}
