package org.leopard.internal.metadata;

import org.leopard.common.metadata.Node;
import org.leopard.common.metadata.Partition;
import org.leopard.internal.config.ServerConfig;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RandomAssignor extends LeaderAssignorAdapter {

    private final Node thisNode;

    public RandomAssignor(ServerConfig config, ClusterNodeCacheWriterSupport nodeWriterSupport) {
        super(config, nodeWriterSupport);
        this.thisNode = nodeWriterSupport.getThisNode();
    }

    @Override
    protected Set<Partition> assign(String topic, int partitionLimit, int replicateLimit) throws Exception {
        Set<Node> nodes = getNodes();

        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalArgumentException("Cluster nodes is empty");
        }

        Set<Partition> partitions = new HashSet<>(partitionLimit);
        for (int i = 0; i < partitionLimit; i++) {
            Node leader = randomAccessNode(nodes, null);

            Partition.PartitionBuilder builder = Partition.newBuilder();
            builder.id(i);
            builder.epoch(0);
            builder.ledgerId(0);
            builder.leader(leader.getName());

            nodes.remove(leader);
            List<String> replicates = new ArrayList<>(replicateLimit);
            for (int j = 0; j < replicateLimit; j++) {
                Node replicateNode = randomAccessNode(nodes, leader.getName());
                replicates.add(replicateNode.getName());
                nodes.remove(replicateNode);
            }
            builder.replicates(replicates);

            partitions.add(builder.build());
        }
        return partitions;
    }

    @Nonnull
    private Node randomAccessNode(Set<Node> nodes, String leader) {
        if (nodes.size() == 1) {
            return nodes.stream().findFirst().get();
        }

        return nodes.stream().filter(node -> !node.getName().equals(leader)).findAny().orElse(thisNode);
    }
}
