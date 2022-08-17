package org.shallow.metadata.management;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.meta.PartitionRecord;
import org.shallow.meta.TopicRecord;
import org.shallow.metadata.MappedFileApi;
import org.shallow.metadata.atomic.DistributedAtomicInteger;
import org.shallow.metadata.sraft.AbstractSRaftLog;
import org.shallow.metadata.sraft.CommitRecord;
import org.shallow.metadata.sraft.CommitType;
import org.shallow.metadata.sraft.SRaftProcessController;
import org.shallow.pool.DefaultFixedChannelPoolFactory;
import org.shallow.proto.PartitionMetadata;
import org.shallow.proto.elector.CreateTopicPrepareCommitRequest;
import org.shallow.proto.elector.CreateTopicPrepareCommitResponse;
import org.shallow.proto.elector.DeleteTopicPrepareCommitRequest;
import org.shallow.proto.elector.DeleteTopicPrepareCommitResponse;
import java.net.SocketAddress;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;

import static org.shallow.metadata.MetadataConstants.TOPICS;
import static org.shallow.util.JsonUtil.object2Json;

public class TopicManager extends AbstractSRaftLog<TopicRecord> {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(TopicManager.class);

    private final LoadingCache<String, Set<PartitionRecord>> topicCommitRecordCache;
    private final LoadingCache<String, Set<PartitionRecord>> topicUnCommitRecordCache;
    private final MappedFileApi api;
    private final DistributedAtomicInteger atomicValue;

    public TopicManager(Set<SocketAddress> quorumVoterAddresses, BrokerConfig config, SRaftProcessController controller) {
        super(quorumVoterAddresses, DefaultFixedChannelPoolFactory.INSTANCE.acquireChannelPool(), config);
        this.api = controller.getMappedFileApi();
        this.atomicValue = controller.getAtomicValue();
        this.topicCommitRecordCache = Caffeine.newBuilder()
                .expireAfterWrite(Long.MAX_VALUE, TimeUnit.DAYS)
                .expireAfterAccess(Long.MAX_VALUE, TimeUnit.DAYS)
                .build(key -> new CopyOnWriteArraySet<>());

        this.topicUnCommitRecordCache = Caffeine.newBuilder()
                .expireAfterWrite(Long.MAX_VALUE, TimeUnit.DAYS)
                .expireAfterAccess(Long.MAX_VALUE, TimeUnit.DAYS)
                .build(key -> new CopyOnWriteArraySet<>());
    }

    @Override
    protected CommitRecord<TopicRecord> doPrepareCommit(TopicRecord record, CommitType type) {
        CommitRecord<TopicRecord> commitRecord = null;
        switch (type) {
            case ADD -> {
                commitRecord = create(record);
            }

            case REMOVE -> {
                commitRecord = delete(record);
            }
        }
        return commitRecord;
    }

    private CommitRecord<TopicRecord> create(TopicRecord record) {
        try {
            topicCommitRecordCache.invalidate(record.getName());

            String topic = record.getName();
            int latencies = record.getLatencies();
            int partitions = record.getPartitions();

            PartitionRecord partitionRecord = new PartitionRecord(atomicValue.increment().preValue(), latencies, elect(), calculateReplicas());
            record.setPartitionRecord(partitionRecord);

            CreateTopicPrepareCommitRequest.Builder requestBuilder = CreateTopicPrepareCommitRequest.newBuilder();
            if (!config.isStandAlone()) {
                requestBuilder.setTopic(topic)
                        .setLatencies(latencies)
                        .setPartitions(partitions)
                        .setPartitionMetadata(PartitionMetadata
                                .newBuilder()
                                .setId(partitionRecord.getId())
                                .setLatency(latencies)
                                .setLeader(partitionRecord.getLeader())
                                .addAllReplicas(partitionRecord.getLatencies())
                                .build());
            }

            CreateTopicPrepareCommitResponse.Builder responseBuilder = CreateTopicPrepareCommitResponse.newBuilder();
            if (config.isStandAlone()) {
                responseBuilder.setTopic(topic)
                        .setLatencies(latencies)
                        .setPartitions(partitions)
                        .setPartitionMetadata(PartitionMetadata
                                .newBuilder()
                                .setId(partitionRecord.getId())
                                .setLatency(latencies)
                                .setLeader(partitionRecord.getLeader())
                                .addAllReplicas(partitionRecord.getLatencies())
                                .build());
            }

            CommitRecord<TopicRecord> commitRecord = new CommitRecord<>(record, CommitType.ADD, requestBuilder.build(), responseBuilder.build());
            Set<PartitionRecord> partitionRecords = topicUnCommitRecordCache.get(record.getName());
            partitionRecords.add(partitionRecord);

            topicUnCommitRecordCache.put(record.getName(), partitionRecords);

            return commitRecord;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create topic<" + record.getName() + ">");
        }
    }

    private String elect() {
        return config.getServerId();
    }

    private List<String> calculateReplicas() {
        return List.of(config.getServerId());
    }

    private CommitRecord<TopicRecord> delete(TopicRecord record) {
        String topic = record.getName();
        topicCommitRecordCache.invalidate(topic);

        DeleteTopicPrepareCommitRequest.Builder requestBuilder = DeleteTopicPrepareCommitRequest.newBuilder();
        if (!config.isStandAlone()) {
            requestBuilder
                    .setTopic(topic)
                    .build();
        }

        DeleteTopicPrepareCommitResponse.Builder responseBuilder = DeleteTopicPrepareCommitResponse.newBuilder();
        if (config.isStandAlone()) {
            responseBuilder
                    .setTopic(topic)
                    .build();
        }

        return new CommitRecord<>(record, CommitType.REMOVE, requestBuilder.build(), responseBuilder.build());
    }

    @Override
    protected void doPostCommit(TopicRecord record, CommitType type) {
        switch (type) {
            case ADD -> {
                topicUnCommitRecordCache.invalidate(record.getName());
                Set<PartitionRecord> partitionRecords = topicUnCommitRecordCache.get(record.getName());
                partitionRecords.add(record.getPartitionRecord());

                topicCommitRecordCache.put(record.getName(), partitionRecords);
            }

            case REMOVE -> {
                topicCommitRecordCache.invalidate(record.getName());
                topicUnCommitRecordCache.invalidate(record.getName());
            }
        }

        String content = object2Json(topicCommitRecordCache.asMap());
        try {
            api.write2File(content, TOPICS);
        } catch (Exception e) {
            if (logger.isErrorEnabled()) {
                logger.error(e.getMessage(), e);
            }
        }
    }
}
