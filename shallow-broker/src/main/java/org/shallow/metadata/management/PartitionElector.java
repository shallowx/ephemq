package org.shallow.metadata.management;

import io.netty.util.collection.IntObjectHashMap;
import org.shallow.internal.BrokerManager;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

import java.util.List;
import java.util.Map;

public class PartitionElector {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(PartitionElector.class);

    private final BrokerConfig config;
    private final BrokerManager manager;

    public PartitionElector(BrokerConfig config, BrokerManager manager) {
        this.config = config;
        this.manager = manager;
    }

    public Map<Integer, ElectorResult> elect(int partitions, int replicas) {
        Map<Integer, ElectorResult> result = new IntObjectHashMap<>();
        for (int i = 0; i < partitions; i++) {
            result.put(i, new ElectorResult(config.getServerId(), calculateReplicas()));
        }
        return result;
    }

    public List<String> calculateReplicas() {
        return List.of(config.getServerId());
    }

    public static class ElectorResult {
        String leader;
        List<String> replicas;

        public ElectorResult(String leader, List<String> replicas) {
            this.leader = leader;
            this.replicas = replicas;
        }

        public String getLeader() {
            return leader;
        }

        public List<String> getReplicas() {
            return replicas;
        }
    }
}
