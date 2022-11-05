package org.shallow.internal.metadata;

import org.shallow.common.logging.InternalLogger;
import org.shallow.common.logging.InternalLoggerFactory;
import org.shallow.internal.BrokerManager;

public class PartitionLeaderElector {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(PartitionLeaderElector.class);

    private final BrokerManager manager;

    public PartitionLeaderElector(BrokerManager manager) {
        this.manager = manager;
    }

    public void elect() throws Exception {

    }
}
