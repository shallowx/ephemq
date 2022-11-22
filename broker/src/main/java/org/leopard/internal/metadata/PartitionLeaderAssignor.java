package org.leopard.internal.metadata;

import org.leopard.common.logging.InternalLogger;
import org.leopard.common.logging.InternalLoggerFactory;
import org.leopard.common.metadata.Node;
import org.leopard.common.metadata.Partition;
import org.leopard.internal.ResourceContext;
import org.leopard.internal.config.ServerConfig;

import java.util.Set;

public class PartitionLeaderAssignor {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(PartitionLeaderAssignor.class);

    private final ServerConfig config;
    private final ClusterNodeCacheWriterSupport nodeWriterSupport;
    private final LeaderAssignorAdapter adapter;

    public PartitionLeaderAssignor(ResourceContext context, ServerConfig config) {
        this.nodeWriterSupport = context.getNodeCacheWriterSupport();
        this.config = config;
        this.adapter = buildAdapter();
    }

    public Set<Partition> assign(String topic, int partitionLimit, int replicateLimit) throws Exception {
        Set<Node> nodes = adapter.getNodes();
        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalArgumentException("Cluster nodes is empty");
        }

        return adapter.assign(topic, partitionLimit, replicateLimit);
    }

    private LeaderAssignorAdapter buildAdapter() {
        String rule = config.getElectAssignRule();
        switch (rule) {
            case PartitionAssignRule.RANDOM -> {
                return new RandomAssignor(config, nodeWriterSupport);
            }

            case PartitionAssignRule.AVERAGE -> {
                return new AverageAssignor(config, nodeWriterSupport);
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
