package org.shallow.metadata.sraft;

import org.shallow.internal.config.BrokerConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

public class StateMachine {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(StateMachine.class);

    private final BrokerConfig config;

    public StateMachine(BrokerConfig config) {
        this.config = config;
    }


}
