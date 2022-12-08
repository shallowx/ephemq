package org.ostara.internal.metadata;

import java.util.Set;
import org.ostara.common.metadata.Node;
import org.ostara.common.metadata.Partition;
import org.ostara.internal.ResourceContext;
import org.ostara.internal.config.ServerConfig;

public abstract class LeaderAssignorAdapter {

    protected final ServerConfig config;
    protected final ClusterNodeCacheSupport nodeWriterSupport;
    protected final ResourceContext context;

    public LeaderAssignorAdapter(ServerConfig config, ResourceContext context) {
        this.config = config;
        this.nodeWriterSupport = context.getNodeCacheSupport();
        this.context = context;
    }

    protected Set<Node> getNodes() throws Exception {
        return nodeWriterSupport.load(config.getClusterName());
    }

    protected abstract Set<Partition> assign(String topic, int partitionLimit, int replicateLimit) throws Exception;
}
