package org.shallow.metadata.sraft;

import io.netty.util.concurrent.EventExecutorGroup;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

public class SRaftProcessController {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(SRaftProcessController.class);

    private final BrokerConfig config;
    private final ProcessRoles role;

    public SRaftProcessController(BrokerConfig config, EventExecutorGroup group) {
        this.config = config;
        this.role = ProcessRoles.Follower;
    }

    private void start() {

    }

    public void shutdownGracefully() {

    }
}
