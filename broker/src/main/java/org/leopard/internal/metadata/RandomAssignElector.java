package org.leopard.internal.metadata;

import org.leopard.common.metadata.Node;
import org.leopard.common.metadata.Partition;
import org.leopard.internal.config.ServerConfig;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RandomAssignElector extends LeaderElectorAdapter {

    public RandomAssignElector(ServerConfig config, ClusterNodeCacheWriterSupport nodeWriterSupport) {
        super(config, nodeWriterSupport);
    }

    @Override
    protected Set<Partition> elect(String topic, int partitionLimit, int replicateLimit) throws Exception {
        Set<Node> nodes = getNodes();

        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalArgumentException("Cluster nodes is empty");
        }

        Set<Partition> partitions = new HashSet<>();
        for (int i = 0; i < partitionLimit; i++) {
            Node leader = randomAccessNode(nodes, null);

            Partition.PartitionBuilder builder = Partition.newBuilder();
            builder.id(i);
            builder.ledgerId(0);
            builder.leader(leader.getName());

            List<String> replicates = new ArrayList<>();
            for (int j = 0; j < replicateLimit; j++) {
                Node replicateNode = randomAccessNode(nodes, leader.getName());
                replicates.add(replicateNode.getName());
            }
            builder.replicates(replicates);

            partitions.add(builder.build());
        }
        return partitions;
    }

    private Node randomAccessNode(Set<Node> nodes, String leader) {
        if (nodes.size() == 1) {
            return nodes.stream().findFirst().get();
        }

        return nodes.stream().filter(node -> !node.getName().equals(leader)).findAny().orElse(null);
    }
}
