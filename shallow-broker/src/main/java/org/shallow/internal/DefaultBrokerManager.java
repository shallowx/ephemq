package org.shallow.internal;

import org.shallow.ClientConfig;
import org.shallow.log.LedgerManager;
import org.shallow.metadata.sraft.SRaftQuorumVoterClient;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.metadata.MappedFileApi;
import org.shallow.metadata.sraft.SRaftProcessController;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DefaultBrokerManager implements BrokerManager {

    private final SRaftQuorumVoterClient client;
    private final MappedFileApi api;
    private final SRaftProcessController controller;
    private final LedgerManager logManager;

    public DefaultBrokerManager(BrokerConfig config) {
        ClientConfig quorumVoterClientConfig = new ClientConfig();

        List<String> quorumVoterAddress = Stream.of(config.getControllerQuorumVoters())
                .map(voters -> {
            final int length = voters.length();
            return voters.substring(voters.lastIndexOf("@") + 1, length);
        }).collect(Collectors.toCollection(LinkedList::new));

        quorumVoterClientConfig.setBootstrapSocketAddress(quorumVoterAddress);
        quorumVoterClientConfig.setChannelFixedPoolCapacity(config.getInternalChannelPoolLimit());
        quorumVoterClientConfig.setInvokeExpiredMs(config.getInvokeTimeMs());

        this.client = new SRaftQuorumVoterClient("quorum-voter-client", quorumVoterClientConfig);
        client.start();

        this.api = new MappedFileApi(config);
        this.controller = new SRaftProcessController(config, this);
        this.logManager = new LedgerManager(config);
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
    public SRaftQuorumVoterClient getQuorumVoterClient() {
        return client;
    }

    @Override
    public SRaftProcessController getController() {
        return controller;
    }

    @Override
    public LedgerManager getLogManager() {
        return logManager;
    }

    @Override
    public void shutdownGracefully() throws Exception {
        controller.shutdownGracefully();
        logManager.close();
    }
}
