package org.shallow.metadata.sraft;

import org.shallow.internal.config.BrokerConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

public class SRaftProcessController {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(SRaftProcessController.class);

    private final BrokerConfig config;

    public SRaftProcessController(BrokerConfig config) {
        this.config = config;
    }

    private void start() {

    }



    public void shutdownGracefully() {

    }
}
