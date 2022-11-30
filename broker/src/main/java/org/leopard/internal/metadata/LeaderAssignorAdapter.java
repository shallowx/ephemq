package org.leopard.internal.metadata;

import java.util.Set;
import org.leopard.common.metadata.Node;
import org.leopard.common.metadata.Partition;
import org.leopard.internal.ResourceContext;
import org.leopard.internal.config.ServerConfig;

public abstract class LeaderAssignorAdapter {

    protected final ServerConfig config;
    protected final ClusterNodeCacheWriterSupport nodeWriterSupport;
    protected final ResourceContext context;

    public LeaderAssignorAdapter(ServerConfig config, ResourceContext context) {
        this.config = config;
        this.nodeWriterSupport = context.getNodeCacheWriterSupport();
        this.context = context;
    }

    protected Set<Node> getNodes() throws Exception {
        return nodeWriterSupport.load(config.getClusterName());
    }

    protected abstract Set<Partition> assign(String topic, int partitionLimit, int replicateLimit) throws Exception;
}
