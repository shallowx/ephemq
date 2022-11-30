package org.leopard.internal.metadata;

import java.util.Set;
import org.leopard.common.logging.InternalLogger;
import org.leopard.common.logging.InternalLoggerFactory;
import org.leopard.common.metadata.Node;
import org.leopard.common.metadata.Partition;
import org.leopard.internal.ResourceContext;
import org.leopard.internal.config.ServerConfig;

public class PartitionLeaderAssignorFactory {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(PartitionLeaderAssignorFactory.class);

    private final ServerConfig config;
    private final LeaderAssignorAdapter adapter;
    private final ResourceContext context;

    public PartitionLeaderAssignorFactory(ResourceContext context, ServerConfig config) {
        this.config = config;
        this.adapter = buildAdapter();
        this.context = context;
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
                return new RandomAssignor(config, context);
            }

            case PartitionAssignRule.AVERAGE -> {
                return new AverageAssignor(config, context);
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
