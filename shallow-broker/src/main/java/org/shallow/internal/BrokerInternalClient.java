package org.shallow.internal;

import org.shallow.ClientConfig;
import org.shallow.pool.ShallowChannelHealthChecker;

public class BrokerInternalClient extends AbstractInternalClient{

    public BrokerInternalClient(String name, ClientConfig config) {
        super(name, config);
    }

    public BrokerInternalClient(String name, ClientConfig config, ShallowChannelHealthChecker healthChecker) {
        super(name, config, healthChecker);
    }

}
