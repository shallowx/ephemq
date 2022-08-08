package org.shallow.metadata.sraft;

import org.shallow.internal.config.BrokerConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

public class SRaftQuorumVoter {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(SRaftQuorumVoter.class);

    private final BrokerConfig config;
    private final ProcessRoles role;

    public SRaftQuorumVoter(BrokerConfig config) {
        this.config = config;
        this.role = ProcessRoles.Follower;
    }
}
