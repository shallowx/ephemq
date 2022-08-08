package org.shallow.internal;

import org.shallow.internal.client.QuorumVoterClient;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.metadata.MappedFileApi;

public class DefaultBrokerManager implements BrokerManager {

    private final BrokerConfig config;
    private final QuorumVoterClient client;
    public final MappedFileApi api;

    public DefaultBrokerManager(BrokerConfig config) {
        this.config = config;
        this.client = new QuorumVoterClient();
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
