package org.shallow.metadata;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.meta.PartitionRecord;
import org.shallow.meta.TopicRecord;
import org.shallow.metadata.sraft.AbstractSRaftLog;
import org.shallow.metadata.sraft.CommitRecord;
import org.shallow.metadata.sraft.CommitType;
import org.shallow.pool.DefaultFixedChannelPoolFactory;
import org.shallow.proto.PartitionMetadata;
import org.shallow.proto.elector.CreateTopicPrepareCommitRequest;
import org.shallow.proto.elector.CreateTopicPrepareCommitResponse;
import org.shallow.proto.elector.DeleteTopicPrepareCommitRequest;
import org.shallow.proto.elector.DeleteTopicPrepareCommitResponse;
import java.net.SocketAddress;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.shallow.metadata.MetadataConstants.TOPICS;
import static org.shallow.util.JsonUtil.object2Json;

public class TopicManager extends AbstractSRaftLog<TopicRecord> {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(TopicManager.class);

    private final LoadingCache<String, PartitionRecord> topicCommitRecordCache;
    private final LoadingCache<String, PartitionRecord> topicUnCommitRecordCache;
    private final MappedFileApi api;

    public TopicManager(Set<SocketAddress> quorumVoterAddresses, BrokerConfig config, MappedFileApi api) {
        super(quorumVoterAddresses, DefaultFixedChannelPoolFactory.INSTANCE.acquireChannelPool(), config);
        this.api = api;
        this.topicCommitRecordCache = Caffeine.newBuilder()
                .expireAfterWrite(Long.MAX_VALUE, TimeUnit.DAYS)
                .expireAfterAccess(Long.MAX_VALUE, TimeUnit.DAYS)
                .build(new CacheLoader<>() {
                    @Override
                    public @Nullable PartitionRecord load(String key) throws Exception {
                        return null;
                    }
                });

        this.topicUnCommitRecordCache = Caffeine.newBuilder()
                .expireAfterWrite(Long.MAX_VALUE, TimeUnit.DAYS)
                .expireAfterAccess(Long.MAX_VALUE, TimeUnit.DAYS)
                .build(new CacheLoader<>() {
                    @Override
                    public @Nullable PartitionRecord load(String key) throws Exception {
                        return null;
                    }
                });
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

            PartitionRecord partitionRecord = new PartitionRecord(-1, latencies, "node1", List.of("node1"));
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
            topicUnCommitRecordCache.put(record.getName(), partitionRecord);

            return commitRecord;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create topic<" + record.getName() + ">");
        }
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
                topicCommitRecordCache.put(record.getName(), record.getPartitionRecord());
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
