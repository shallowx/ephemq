package org.shallow.internal.client;

import org.shallow.ClientConfig;
import org.shallow.internal.BrokerManager;
import org.shallow.internal.client.AbstractInternalClient;
import org.shallow.pool.ShallowChannelHealthChecker;

public class Client2Broker extends AbstractInternalClient {

    private final BrokerManager brokerManager;

    public Client2Broker(String name, ClientConfig config, BrokerManager brokerManager) {
        super(name, config);
        this.brokerManager = brokerManager;
    }

    public Client2Broker(String name, ClientConfig config, BrokerManager brokerManager, ShallowChannelHealthChecker healthChecker) {
        super(name, config, healthChecker);
        this.brokerManager = brokerManager;
    }
}
