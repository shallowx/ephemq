package org.shallow.pool;

import io.netty.bootstrap.Bootstrap;
import org.shallow.ClientConfig;

public class DefaultChannelPoolFactory {

    public static final DefaultChannelPoolFactory INSTANCE = new DefaultChannelPoolFactory();
    private ShallowChannelPool pool;

    public ShallowChannelPool newChannelPool(Bootstrap bootstrap, ClientConfig config) {
        return newChannelPool(bootstrap, config, null);
    }

    public ShallowChannelPool newChannelPool(Bootstrap bootstrap, ClientConfig config, ShallowChannelHealthChecker healthChecker) {
        pool = new DynamicChannelPool(bootstrap, config, (healthChecker == null ? ShallowChannelHealthChecker.ACTIVE : healthChecker));
        return pool;
    }

    public ShallowChannelPool acquireChannelPool() {
        return pool;
    }
}
