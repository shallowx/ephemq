package org.shallow.metadata.snapshot;

import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.meta.NodeRecord;
import org.shallow.metadata.sraft.RaftVoteProcessor;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class PartitionLeaderElector implements LeaderElection {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(PartitionLeaderElector.class);

    private final ClusterSnapshot clusterSnapshot;

    public PartitionLeaderElector(RaftVoteProcessor voteProcessor) {
        this.clusterSnapshot = voteProcessor.getClusterSnapshot();
    }

    @Override
    public String elect(String topic, int partitions, int latencies) throws Exception{
        List<NodeRecord> nodeRecords = clusterSnapshot.checkIfEnough(false, latencies).stream().toList();
        int index =  (((31 * topic.hashCode() + partitions + latencies)) & 0x7fffffff) % nodeRecords.size();
        return nodeRecords.get(index).getName();
    }

    @Override
    public List<String> assignLatencies(String topic, int partitions, int latencies) throws Exception {
        List<NodeRecord> nodeRecords = clusterSnapshot.checkIfEnough(true, latencies).stream().toList();
        int size = nodeRecords.size();
        int index = ThreadLocalRandom.current().nextInt(size);

        List<String> replicas = new LinkedList<>();
        for (int i = 0; i < latencies; i++) {
            String replica = nodeRecords.get(index).getName();
            for (;;) {
                if (!replicas.contains(replica)) {
                    replicas.add(replica);
                    break;
                }
                index = (index == size -1) ? 0 : index + 1;
            }
        }

        return replicas;
    }



}
