package org.leopard.internal.metadata;

import org.leopard.common.logging.InternalLogger;
import org.leopard.common.logging.InternalLoggerFactory;
import org.leopard.internal.BrokerManager;

public class PartitionLeaderElector {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(PartitionLeaderElector.class);

    private final BrokerManager manager;

    public PartitionLeaderElector(BrokerManager manager) {
        this.manager = manager;
    }

    public void elect() throws Exception {

    }
}
