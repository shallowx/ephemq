package org.shallow.internal;

import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutor;

public class DefaultBrokerManager implements BrokerManager {

    private final BrokerConfig config;

    public DefaultBrokerManager(BrokerConfig config) {
        this.config = config;
    }

    private EventExecutor newEventExecutor(final String name) {
        return new DefaultEventExecutor(new DefaultThreadFactory(name));
    }


    @Override
    public void start() throws Exception {

    }

    @Override
    public void shutdownGracefully() throws Exception {

    }
}
