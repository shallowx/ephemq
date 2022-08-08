package org.shallow.internal;

import org.shallow.internal.client.Client2QuorumController;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.metadata.MappedFileApi;

public class DefaultBrokerManager implements BrokerManager {

    private final BrokerConfig config;
    private final Client2QuorumController client;
    public final MappedFileApi api;

    public DefaultBrokerManager(BrokerConfig config) {
        this.config = config;
        this.client = new Client2QuorumController();
        this.api = new MappedFileApi(config);
    }

    @Override
    public void start() throws Exception {
        api.start();
    }

    @Override
    public void shutdownGracefully() throws Exception {

    }
}
