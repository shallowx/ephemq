package org.shallow.internal.client;

import org.shallow.ClientConfig;
import org.shallow.internal.BrokerManager;
import org.shallow.internal.client.AbstractInternalClient;
import org.shallow.pool.ShallowChannelHealthChecker;

public class Client2Nameserver extends AbstractInternalClient {

    private final BrokerManager brokerManager;

    public Client2Nameserver(String name, ClientConfig config, BrokerManager brokerManager) {
        super(name, config);
        this.brokerManager = brokerManager;
    }

    public Client2Nameserver(String name, ClientConfig config, ShallowChannelHealthChecker healthChecker, BrokerManager brokerManager) {
        super(name, config, healthChecker);
        this.brokerManager = brokerManager;
    }
}
