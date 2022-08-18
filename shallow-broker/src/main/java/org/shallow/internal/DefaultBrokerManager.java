package org.shallow.internal;

import org.shallow.ClientConfig;
import org.shallow.internal.client.QuorumVoterClient;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.metadata.MappedFileApi;
import org.shallow.metadata.sraft.SRaftProcessController;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DefaultBrokerManager implements BrokerManager {

    private final BrokerConfig config;
    private final QuorumVoterClient client;
    private final MappedFileApi api;
    private final SRaftProcessController controller;

    public DefaultBrokerManager(BrokerConfig config) {
        this.config = config;

        final ClientConfig quorumVoterClientConfig = new ClientConfig();
        quorumVoterClientConfig.setInvokeExpiredMs(config.getInvokeTimeMs());
        final List<String> quorumVoterAddress = Stream.of(config.getControllerQuorumVoters()).map(voters -> {
            final int length = voters.length();
            return voters.substring(voters.lastIndexOf("@") + 1, length);
        }).collect(Collectors.toCollection(LinkedList::new));

        quorumVoterClientConfig.setBootstrapSocketAddress(quorumVoterAddress);
        this.client = new QuorumVoterClient("quorum-voter-client", quorumVoterClientConfig);
        client.start();

        this.api = new MappedFileApi(config);
        this.controller = new SRaftProcessController(config, this);
    }

    @Override
    public void start() throws Exception {
        controller.start();
        api.start();
    }

    @Override
    public MappedFileApi getMappedFileApi() {
        return api;
    }

    @Override
    public QuorumVoterClient getQuorumVoterClient() {
        return client;
    }

    @Override
    public SRaftProcessController getController() {
        return controller;
    }

    @Override
    public void shutdownGracefully() throws Exception {
        controller.shutdownGracefully();
    }
}
