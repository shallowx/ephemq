package org.shallow.internal;

import org.shallow.ClientConfig;
import org.shallow.log.LedgerManager;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.metadata.sraft.RaftVoteProcessor;
import org.shallow.network.BrokerConnectionManager;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DefaultBrokerManager implements BrokerManager {

    private final LedgerManager logManager;
    private final BrokerConnectionManager connectionManager;
    private final RaftVoteProcessor voteProcessor;

    public DefaultBrokerManager(BrokerConfig config) throws Exception {
        ClientConfig quorumVoterClientConfig = new ClientConfig();

        List<String> quorumVoterAddress = Stream.of(config.getControllerQuorumVoters())
                .map(voters -> {
            final int length = voters.length();
            return voters.substring(voters.lastIndexOf("@") + 1, length);
        }).collect(Collectors.toCollection(LinkedList::new));

        quorumVoterClientConfig.setBootstrapSocketAddress(quorumVoterAddress);
        quorumVoterClientConfig.setChannelFixedPoolCapacity(config.getInternalChannelPoolLimit());
        quorumVoterClientConfig.setInvokeExpiredMs(config.getInvokeTimeMs());

        this.logManager = new LedgerManager(config);
        this.connectionManager = new BrokerConnectionManager();
        this.voteProcessor = new RaftVoteProcessor(config, this);
    }

    @Override
    public void start() throws Exception {
        voteProcessor.start();
        logManager.start();
    }

    @Override
    public LedgerManager getLedgerManager() {
        return logManager;
    }

    @Override
    public BrokerConnectionManager getBrokerConnectionManager() {
        return connectionManager;
    }

    @Override
    public RaftVoteProcessor getVoteProcessor() {
        return voteProcessor;
    }

    @Override
    public void shutdownGracefully() throws Exception {
        logManager.close();
    }
}
