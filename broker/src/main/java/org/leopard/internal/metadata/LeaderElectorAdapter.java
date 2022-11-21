package org.leopard.internal.metadata;

import org.leopard.common.metadata.Node;
import org.leopard.common.metadata.Partition;
import org.leopard.internal.config.ServerConfig;

import java.util.Set;

public abstract class LeaderElectorAdapter {

    private final ServerConfig config;
    private final ClusterNodeCacheWriterSupport nodeWriterSupport;

    public LeaderElectorAdapter(ServerConfig config, ClusterNodeCacheWriterSupport nodeWriterSupport) {
        this.config = config;
        this.nodeWriterSupport = nodeWriterSupport;
    }

    protected Set<Node> getNodes() throws Exception {
        return nodeWriterSupport.load(config.getClusterName());
    }

    protected abstract Set<Partition> elect(String topic, int partitionLimit, int replicateLimit) throws Exception;
}
