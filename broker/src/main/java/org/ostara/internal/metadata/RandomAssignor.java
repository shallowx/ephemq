package org.ostara.internal.metadata;

import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import org.ostara.common.metadata.Node;
import org.ostara.common.metadata.Partition;
import org.ostara.internal.ResourceContext;
import org.ostara.internal.atomic.DistributedAtomicInteger;
import org.ostara.internal.config.ServerConfig;

public class RandomAssignor extends LeaderAssignorAdapter {

    private final Node thisNode;
    private final DistributedAtomicInteger distributedAtomicInteger;

    public RandomAssignor(ServerConfig config, ResourceContext context) {
        super(config, context);
        this.thisNode = nodeWriterSupport.getThisNode();
        this.distributedAtomicInteger = context.getAtomicInteger();
    }

    @Override
    protected Set<Partition> assign(String topic, int partitionLimit, int replicateLimit) throws Exception {
        Set<Node> nodes = getNodes();

        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalArgumentException("Cluster nodes is empty");
        }

        Set<Node> newNodes = Sets.newHashSet(nodes);
        Set<Partition> partitions = new HashSet<>(partitionLimit);
        for (int i = 0; i < partitionLimit; i++) {
            Node leader = randomAccessNode(newNodes, null);

            Partition.PartitionBuilder builder = Partition.newBuilder();
            builder.id(i);
            builder.epoch(-1);
            builder.ledgerId(distributedAtomicInteger.increment().postValue());
            builder.leader(leader.getName());

            newNodes.remove(leader);
            List<String> replicates = new ArrayList<>(replicateLimit);
            for (int j = 0; j < replicateLimit; j++) {
                Node replicateNode = randomAccessNode(newNodes, leader.getName());
                replicates.add(replicateNode.getName());
                newNodes.remove(replicateNode);
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
