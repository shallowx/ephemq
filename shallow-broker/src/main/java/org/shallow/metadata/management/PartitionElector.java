package org.shallow.metadata.management;

import io.netty.util.collection.IntObjectHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
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

    public Int2ObjectMap<ElectorRecord> elect(int partitions, int replicas) {
        Int2ObjectMap<ElectorRecord> result = new Int2ObjectOpenHashMap<>();
        for (int i = 0; i < partitions; i++) {
            for (int j = 0; j < replicas; j++) {
                result.put(i, new ElectorRecord(config.getServerId(), assignLatencies()));
            }
        }
        return result;
    }

    public List<String> assignLatencies() {
        return List.of(config.getServerId());
    }
}
