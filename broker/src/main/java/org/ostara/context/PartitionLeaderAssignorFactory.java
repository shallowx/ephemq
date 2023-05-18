package org.ostara.context;

import java.util.Set;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.common.metadata.Node;
import org.ostara.common.metadata.Partition;
import org.ostara.config.ServerConfig;

public class PartitionLeaderAssignorFactory {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(PartitionLeaderAssignorFactory.class);

    private final ServerConfig config;
    private final LeaderAssignorAdapter adapter;
    private final ResourceContext context;

    public PartitionLeaderAssignorFactory(ResourceContext context, ServerConfig config) {
        this.config = config;
        this.context = context;
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
                return new RandomAssignor(config, context);
            }

            case PartitionAssignRule.AVERAGE -> {
                return new AverageAssignor(config, context);
            }

            default -> {
                logger.error("Unsupported partition leader elect operation type: " + rule);
                throw new UnsupportedOperationException("Unsupported partition leader elect operation type: " + rule);
            }
        }
    }
}
