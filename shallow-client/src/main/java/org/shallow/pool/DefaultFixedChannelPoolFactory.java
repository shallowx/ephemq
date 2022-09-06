package org.shallow.pool;

import org.shallow.Client;

public class DefaultFixedChannelPoolFactory {

    public static final DefaultFixedChannelPoolFactory INSTANCE = new DefaultFixedChannelPoolFactory();
    private ShallowChannelPool pool;

    public ShallowChannelPool newChannelPool(Client client) throws Exception {
        return newChannelPool(client, client.getHealthChecker());
    }

    public ShallowChannelPool newChannelPool(Client client,  ShallowChannelHealthChecker healthChecker) throws Exception {
        pool = new FixedChannelPool(client, (healthChecker == null ? ShallowChannelHealthChecker.ACTIVE : healthChecker));
        pool.initChannelPool();
        return pool;
    }

    public ShallowChannelPool acquireChannelPool() {
        return pool;
    }
}
