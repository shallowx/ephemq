package org.shallow.metadata;

import org.shallow.Manager;
import org.shallow.common.logging.InternalLogger;
import org.shallow.common.logging.InternalLoggerFactory;

public class HeartbeatManager {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(HeartbeatManager.class);

    private final Manager manager;

    public HeartbeatManager(Manager manager) {
        this.manager = manager;
    }
}
