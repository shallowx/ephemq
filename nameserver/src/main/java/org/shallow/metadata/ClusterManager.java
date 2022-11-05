package org.shallow.metadata;

import org.shallow.Manager;
import org.shallow.common.logging.InternalLogger;
import org.shallow.common.logging.InternalLoggerFactory;

public class ClusterManager {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ClusterManager.class);

    private final Manager manager;

    public ClusterManager(Manager manager) {
        this.manager = manager;
    }

    public void register() {

    }

    public void unregister() {

    }
}
