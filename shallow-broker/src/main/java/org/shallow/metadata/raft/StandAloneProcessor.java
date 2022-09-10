package org.shallow.metadata.raft;

import org.shallow.internal.config.BrokerConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

import java.util.List;
import java.util.function.Supplier;

public class StandAloneProcessor {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(StandAloneProcessor.class);

    private final List<String> quorumVoteAddresses;
    private final BrokerConfig config;

    public StandAloneProcessor(List<String> quorumVoteAddresses, BrokerConfig config) {
        this.quorumVoteAddresses = quorumVoteAddresses;
        this.config = config;
    }

    public void start(Supplier<Void> supplier) throws Exception {
        int size = quorumVoteAddresses.size();
        if (size > 1 && config.isStandAlone()) {
            throw new IllegalArgumentException("Quorum address size > 1, but expect = 1");
        }

        if (!config.isStandAlone()) {
            supplier.get();
        }

        if (logger.isInfoEnabled()) {
            logger.info("This node is elected as the leader, name={} host={} port={}", config.getServerId(), config.getExposedHost(), config.getExposedPort());
        }
    }
}
