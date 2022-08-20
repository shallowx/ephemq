package org.shallow.metadata.management;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.shallow.internal.BrokerManager;
import org.shallow.log.Log;
import org.shallow.log.LogManager;
import org.shallow.metadata.sraft.SRaftQuorumVoterClient;
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
import static org.shallow.metadata.sraft.CommitType.ADD;
import static org.shallow.metadata.sraft.CommitType.REMOVE;
import static org.shallow.util.JsonUtil.object2Json;
import static org.shallow.util.ObjectUtil.isNull;

public class TopicManager extends AbstractSRaftLog<TopicRecord> {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(TopicManager.class);

    private final MappedFileApi api;
    private final DistributedAtomicInteger atomicValue;
    private final SRaftQuorumVoterClient client;
    private final LoadingCache<String, Set<PartitionRecord>> commitRecordCache;
    private final LoadingCache<String, Set<PartitionRecord>> unCommitRecordCache;
    private final BrokerManager manager;

    public TopicManager(Set<SocketAddress> quorumVoterAddresses, BrokerConfig config, BrokerManager manager) {
        super(quorumVoterAddresses, DefaultFixedChannelPoolFactory.INSTANCE.acquireChannelPool(), config, manager.getController());
        this.manager = manager;
        this.api = manager.getMappedFileApi();
        this.atomicValue = controller.getAtomicValue();
        this.client = manager.getQuorumVoterClient();

        this.commitRecordCache = Caffeine.newBuilder()
                .expireAfterWrite(Long.MAX_VALUE, TimeUnit.DAYS)
                .expireAfterAccess(Long.MAX_VALUE, TimeUnit.DAYS)
                .build(new CacheLoader<>() {
                    @Override
                    public @Nullable Set<PartitionRecord> load(String key) throws Exception {
                        return syncFromQuorumLeader(key);
                    }
                });

        this.unCommitRecordCache = Caffeine.newBuilder()
                .expireAfterWrite(Long.MAX_VALUE, TimeUnit.DAYS)
                .expireAfterAccess(Long.MAX_VALUE, TimeUnit.DAYS)
                .build(new CacheLoader<>() {
                    @Override
                    public @Nullable Set<PartitionRecord> load(String key) throws Exception {
                        return null;
                    }
                });
    }

    @Override
    protected CommitRecord<TopicRecord> doPrepareCommit(TopicRecord record, CommitType type) {
        CommitRecord<TopicRecord> commitRecord = null;
        switch (type) {
            case ADD -> {
                commitRecord = preCreate(record);
            }

            case REMOVE -> {
                commitRecord = preDelete(record);
            }
        }
        return commitRecord;
    }

    private CommitRecord<TopicRecord> preCreate(TopicRecord record) {
        String topic = record.getName();
        if (contains(topic)) {
            throw new RuntimeException("The topic<" + topic + "> already exists");
        }

        try {
            commitRecordCache.invalidate(topic);
            int latencies = record.getLatencies();
            int partitions = record.getPartitions();

            PartitionRecord partitionRecord = new PartitionRecord(atomicValue.increment().preValue(), latencies, elect(), calculateReplicas());
            record.setPartitionRecord(partitionRecord);

            CreateTopicPrepareCommitRequest.Builder requestBuilder = CreateTopicPrepareCommitRequest.newBuilder();
            if (!controller.isQuorumLeader()) {
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
            if (controller.isQuorumLeader()) {
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

            CommitRecord<TopicRecord> commitRecord = new CommitRecord<>(record, ADD, requestBuilder.build(), responseBuilder.build());
            Set<PartitionRecord> partitionRecords = commitRecordCache.get(record.getName());
            if (isNull(partitionRecords)) {
                partitionRecords = new CopyOnWriteArraySet<>();
            }
            partitionRecords.add(partitionRecord);

            unCommitRecordCache.put(record.getName(), partitionRecords);

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

    private CommitRecord<TopicRecord> preDelete(TopicRecord record) {
        String topic = record.getName();
        commitRecordCache.invalidate(topic);

        DeleteTopicPrepareCommitRequest.Builder requestBuilder = DeleteTopicPrepareCommitRequest.newBuilder();
        if (!controller.isQuorumLeader()) {
            requestBuilder
                    .setTopic(topic)
                    .build();
        }

        DeleteTopicPrepareCommitResponse.Builder responseBuilder = DeleteTopicPrepareCommitResponse.newBuilder();
        if (controller.isQuorumLeader()) {
            responseBuilder
                    .setTopic(topic)
                    .build();
        }

        return new CommitRecord<>(record, REMOVE, requestBuilder.build(), responseBuilder.build());
    }

    @Override
    protected void doPostCommit(TopicRecord record, CommitType type) {
        switch (type) {
            case ADD -> {
                unCommitRecordCache.invalidate(record.getName());
                Set<PartitionRecord> partitionRecords = unCommitRecordCache.get(record.getName());
                if (isNull(partitionRecords)) {
                    partitionRecords = new CopyOnWriteArraySet<>();
                }
                partitionRecords.add(record.getPartitionRecord());

                commitRecordCache.put(record.getName(), partitionRecords);

                //TODO notify init log
                LogManager logManager = manager.getLogManager();
                logManager.initLog();
            }

            case REMOVE -> {
                String topic = record.getName();
                commitRecordCache.invalidate(topic);
                unCommitRecordCache.invalidate(topic);
            }
        }

        String content = object2Json(commitRecordCache.asMap());
        try {
            api.write2File(content, TOPICS);
        } catch (Exception e) {
            if (logger.isErrorEnabled()) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    public Set<PartitionRecord> getTopicInfo(String topic) {
        Set<PartitionRecord> partitionRecords = commitRecordCache.get(topic);
        if (isNull(partitionRecords) || partitionRecords.isEmpty()) {
            return null;
        }
        return partitionRecords;
    }

    private Set<PartitionRecord> syncFromQuorumLeader(String topic) {
        if (controller.isQuorumLeader()) {
            return null;
        }

        SocketAddress leader = controller.getMetadataLeader();

        return client.queryTopicInfo(topic, leader);
    }

    private boolean contains(String topic) {
        return commitRecordCache.asMap().containsKey(topic);
    }
}
