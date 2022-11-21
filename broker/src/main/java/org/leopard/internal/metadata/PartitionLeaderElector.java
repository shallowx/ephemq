package org.leopard.internal.metadata;

import org.leopard.common.logging.InternalLogger;
import org.leopard.common.logging.InternalLoggerFactory;
import org.leopard.common.metadata.Node;
import org.leopard.common.metadata.Partition;
import org.leopard.internal.ResourceContext;
import org.leopard.internal.config.ServerConfig;

import java.util.Set;

public class PartitionLeaderElector {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(PartitionLeaderElector.class);

    private final ServerConfig config;
    private final ClusterNodeCacheWriterSupport nodeWriterSupport;
    private final LeaderElectorAdapter adapter;

    public PartitionLeaderElector(ResourceContext context, ServerConfig config) {
        this.nodeWriterSupport = context.getNodeCacheWriterSupport();
        this.config = config;
        this.adapter = buildAdapter();
    }

    public Set<Partition> elect(String topic, int partitionLimit, int replicateLimit) throws Exception {
        Set<Node> nodes = adapter.getNodes();
        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalArgumentException("Cluster nodes is empty");
        }

        return adapter.elect(topic, partitionLimit, replicateLimit);
    }

    private LeaderElectorAdapter buildAdapter() {
        String rule = config.getElectAssignRule();
        switch (rule) {
            case ElectAssignRule.RANDOM -> {
                return new RandomAssignElector(config, nodeWriterSupport);
            }

            case ElectAssignRule.AVERAGE -> {
                return new AverageAssignElector(config, nodeWriterSupport);
            }

            default -> {
                if (logger.isErrorEnabled()) {
                    logger.error("Unsupported partition leader elect operation type: " + rule);
                }
                
                throw new UnsupportedOperationException("Unsupported partition leader elect operation type: " + rule);
            }
        }
    }
}
