package org.shallow.pool;

import io.netty.bootstrap.Bootstrap;
import org.shallow.ClientConfig;

public class ChannelPoolFactory {

    public static final ChannelPoolFactory INSTANCE = new ChannelPoolFactory();
    private ShallowChannelPool pool;

    public ShallowChannelPool newChannelPool(Bootstrap bootstrap, ClientConfig config) {
        return newChannelPool(bootstrap, config, null);
    }

    public ShallowChannelPool newChannelPool(Bootstrap bootstrap, ClientConfig config, ShallowChannelHealthChecker healthChecker) {
        pool = new DynamicChannelPool(bootstrap, config, (healthChecker == null ? ShallowChannelHealthChecker.ACTIVE : healthChecker));
        return pool;
    }

    public ShallowChannelPool obtainChannelPool() {
        return pool;
    }
}
