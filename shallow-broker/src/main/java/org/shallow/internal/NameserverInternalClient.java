package org.shallow.internal;

import org.shallow.ClientConfig;
import org.shallow.pool.ShallowChannelHealthChecker;

public class NameserverInternalClient extends AbstractInternalClient {

    public NameserverInternalClient(String name, ClientConfig config) {
        super(name, config);
    }

    public NameserverInternalClient(String name, ClientConfig config, ShallowChannelHealthChecker healthChecker) {
        super(name, config, healthChecker);
    }
}
