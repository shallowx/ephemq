package org.shallow.internal;

import org.checkerframework.checker.units.qual.C;
import org.shallow.ClientConfig;
import org.shallow.internal.client.QuorumVoterClient;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.metadata.MappedFileApi;
import org.shallow.metadata.sraft.SRaftProcessController;
import org.shallow.util.NetworkUtil;

import java.net.SocketAddress;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.shallow.util.NetworkUtil.switchSocketAddress;

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
        }).collect(Collectors.toList());

        quorumVoterClientConfig.setBootstrapSocketAddress(quorumVoterAddress);
        this.client = new QuorumVoterClient("quorum-voter-client", quorumVoterClientConfig);


        this.api = new MappedFileApi(config);
        this.controller = new SRaftProcessController(config, api);
    }

    @Override
    public void start() throws Exception {
        client.start();

        controller.start();
        api.start();
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
