package org.ostara.context;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import org.ostara.common.metadata.Node;
import org.ostara.common.metadata.Partition;
import org.ostara.atomic.DistributedAtomicInteger;
import org.ostara.config.ServerConfig;

public class AverageAssignor extends LeaderAssignorAdapter {

    public AverageAssignor(ServerConfig config, ResourceContext context) {
        super(config, context);
    }

    @Override
    protected Set<Partition> assign(String topic, int partitionLimit, int replicateLimit) throws Exception {
        Set<Node> nodes = getNodes();

        DistributedAtomicInteger atomic = context.getAtomicInteger();
        List<Node> candidateNodes = Lists.newLinkedList(nodes);

        Set<Partition> partitions = new HashSet<>(partitionLimit);
        for (int i = 0; i < partitionLimit; i++) {
            Node leader = randomAccessNode(candidateNodes, null);

            Partition.PartitionBuilder builder = Partition.newBuilder();
            builder.id(i);
            builder.epoch(-1);
            builder.ledgerId(atomic.increment().postValue());
            builder.leader(leader.getName());

            candidateNodes.remove(leader);
            List<String> replicates = new ArrayList<>(replicateLimit);
            for (int j = 0; j < replicateLimit; j++) {
                Node replicateNode = randomAccessNode(candidateNodes, leader.getName());
                replicates.add(replicateNode.getName());
                candidateNodes.remove(replicateNode);
            }
            builder.replicates(replicates);

            partitions.add(builder.build());
        }
        return partitions;
    }

    @Nonnull
    private Node randomAccessNode(List<Node> nodes, String leader) {
        if (nodes.size() == 1) {
            return nodes.stream().findFirst().get();
        }

        return nodes.stream().filter(node -> !node.getName().equals(leader)).findFirst()
                .orElse(context.getNodeCacheSupport().getThisNode());
    }
}
