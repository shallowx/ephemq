package org.leopard.internal.metadata;

import org.leopard.common.logging.InternalLogger;
import org.leopard.common.logging.InternalLoggerFactory;
import org.leopard.common.metadata.PartitionRecord;
import org.leopard.internal.ResourceContext;

import java.util.Set;

public class PartitionLeaderElector {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(PartitionLeaderElector.class);

    private final ResourceContext manager;

    public PartitionLeaderElector(ResourceContext manager) {
        this.manager = manager;
    }

    public Set<PartitionRecord> elect(String topic, int partitions, int latencies) throws Exception {
        return null;
    }
}
