package org.shallow.metadata.snapshot;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.shallow.internal.BrokerManager;
import org.shallow.internal.atomic.DistributedAtomicInteger;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.log.LedgerManager;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.meta.PartitionRecord;
import org.shallow.meta.TopicRecord;
import org.shallow.metadata.MappingFileProcessor;
import org.shallow.metadata.raft.RaftVoteProcessor;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

public class TopicSnapshot {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(TopicSnapshot.class);

    private final MappingFileProcessor fileProcessor;
    private final BrokerConfig config;
    private final DistributedAtomicInteger atomicValue;
    private final LoadingCache<String, TopicRecord> topics;
    private final RaftVoteProcessor voteProcessor;
    private final PartitionLeaderElector leaderElector;
    private final LedgerManager ledgerManager;

    public TopicSnapshot(MappingFileProcessor processor, BrokerConfig config, DistributedAtomicInteger atomicValue, RaftVoteProcessor voteProcessor, BrokerManager manager) {
        this.fileProcessor = processor;
        this.config = config;
        this.atomicValue = atomicValue;
        this.ledgerManager = manager.getLedgerManager();
        this.voteProcessor = voteProcessor;
        this.leaderElector = new PartitionLeaderElector(voteProcessor);
        this.topics = Caffeine.newBuilder().build(new CacheLoader<>() {
            @Override
            public @Nullable TopicRecord load(String key) throws Exception {
                return applyFromNameServer(key);
            }
        });
    }

    private TopicRecord applyFromNameServer(String cluster) {
        return null;
    }

    public void start() throws Exception {

    }

    public void create(String topic, int partitions, int latencies) throws Exception {
        if (partitions <= 0 || latencies<= 0) {
            throw new IllegalArgumentException(String.format("Partitions expect > 0, latency expect > 0, but partitions:%d latency:%d", partitions, latencies));
        }

        Set<PartitionRecord> partitionRecords = new CopyOnWriteArraySet<>();
        for (int i = 0; i < partitions; i++) {
            List<String> replicas = leaderElector.assignLatencies(topic, partitions, latencies);
            String leader = leaderElector.elect(topic, partitions, latencies);

            PartitionRecord partitionRecord = PartitionRecord
                    .newBuilder()
                    .id(atomicValue.increment().preValue())
                    .latency(i)
                    .latencies(replicas)
                    .leader(leader)
                    .build();
            ledgerManager.initLog(topic, i, -1);
            partitionRecords.add(partitionRecord);
        }

        TopicRecord record = TopicRecord
                .newBuilder()
                .name(topic)
                .partitions(partitions)
                .latencies(latencies)
                .partitionRecords(partitionRecords)
                .build();

        topics.put(topic, record);

        fileProcessor.write(JSON.toJSONString(topics.asMap(), SerializerFeature.PrettyFormat, SerializerFeature.WriteMapNullValue, SerializerFeature.WriteDateUseDateFormat));

    }

    public TopicRecord getRecord(String topic) {
        return topics.get(topic);
    }
}
