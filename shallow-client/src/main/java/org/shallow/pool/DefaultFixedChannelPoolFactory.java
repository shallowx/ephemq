package org.shallow.pool;

import io.netty.bootstrap.Bootstrap;
import org.shallow.Client;
import org.shallow.ClientConfig;

public class DefaultFixedChannelPoolFactory {

    public static final DefaultFixedChannelPoolFactory INSTANCE = new DefaultFixedChannelPoolFactory();
    private ShallowChannelPool pool;

    public ShallowChannelPool newChannelPool(Client client) {
        return newChannelPool(client, null);
    }

    public ShallowChannelPool newChannelPool(Client client,  ShallowChannelHealthChecker healthChecker) {
        pool = new FixedChannelPool(client, (healthChecker == null ? ShallowChannelHealthChecker.ACTIVE : healthChecker));
        return pool;
    }

    public ShallowChannelPool acquireChannelPool() {
        return pool;
    }
}
