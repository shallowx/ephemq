package org.shallow.internal;

import org.shallow.Client;
import org.shallow.ClientConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.pool.ShallowChannelHealthChecker;

public abstract class AbstractInternalClient extends Client {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(AbstractInternalClient.class);

    private String name;

    public AbstractInternalClient(String name, ClientConfig config) {
        super(name, config);
        this.name = name;
    }

    public AbstractInternalClient(String name, ClientConfig config, ShallowChannelHealthChecker healthChecker) {
        super(name, config, healthChecker);
        this.name = name;
    }

    @Override
    public void start() {
        super.start();
    }
}
