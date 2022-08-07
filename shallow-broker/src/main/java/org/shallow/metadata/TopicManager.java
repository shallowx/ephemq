package org.shallow.metadata;

import org.shallow.internal.BrokerManager;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

public class TopicManager {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(TopicManager.class);

    private final BrokerManager manager;

    public TopicManager(BrokerManager manager) {
        this.manager = manager;
    }
}
