package org.leopard.internal.metadata;

import org.leopard.common.metadata.Partition;
import org.leopard.internal.config.ServerConfig;

import java.util.Set;

public class AverageAssignElector extends LeaderElectorAdapter {

    public AverageAssignElector(ServerConfig config, ClusterNodeCacheWriterSupport nodeWriterSupport) {
        super(config, nodeWriterSupport);
    }

    @Override
    protected Set<Partition> elect(String topic, int partitionLimit, int replicateLimit) throws Exception {
        return null;
    }
}
