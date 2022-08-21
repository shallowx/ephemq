package org.shallow.metadata.management;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.shallow.internal.BrokerManager;
import org.shallow.log.LogManager;
import org.shallow.metadata.sraft.SRaftQuorumVoterClient;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.meta.PartitionRecord;
import org.shallow.meta.TopicRecord;
import org.shallow.metadata.MappedFileApi;
import org.shallow.internal.atomic.DistributedAtomicInteger;
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
import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
    private final PartitionElector elector;

    public TopicManager(Set<SocketAddress> quorumVoterAddresses, BrokerConfig config, BrokerManager manager) {
        super(quorumVoterAddresses, DefaultFixedChannelPoolFactory.INSTANCE.acquireChannelPool(), config, manager.getController());
        this.manager = manager;
        this.api = manager.getMappedFileApi();
        this.atomicValue = controller.getAtomicValue();
        this.client = manager.getQuorumVoterClient();
        this.elector = new PartitionElector(config, manager);

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
        int partitions = record.getPartitions();
        int latencies = record.getLatencies();
        int clusterSize = manager.getController().getClusterManager().getClusterSize(config.getClusterName());

        if (partitions <= 0 ){
            throw new RuntimeException(String.format("The number of partitions expected: > 0, but actually=%d", partitions));
        }

        if (latencies > clusterSize) {
            throw new RuntimeException(String.format("Not enough cluster nodes, expected=%d actually=%d", latencies, clusterSize));
        }

        if (contains(topic)) {
            throw new RuntimeException("The topic<" + topic + "> already exists");
        }

        try {
            commitRecordCache.invalidate(topic);

            Set<PartitionRecord> partitionRecords = new HashSet<>();
            Map<Integer, PartitionElector.ElectorResult> result = elector.elect(partitions, latencies);
            for (Map.Entry<Integer, PartitionElector.ElectorResult> entry : result.entrySet()) {
                int id = entry.getKey();
                PartitionElector.ElectorResult electorResult = entry.getValue();
                String leader = electorResult.getLeader();
                List<String> replicas = electorResult.getReplicas();

                PartitionRecord partitionRecord = new PartitionRecord(id, atomicValue.increment().preValue(), leader, replicas);
                partitionRecords.add(partitionRecord);
            }
            record.setPartitionRecords(partitionRecords);

            CreateTopicPrepareCommitRequest.Builder requestBuilder = CreateTopicPrepareCommitRequest.newBuilder();
            if (!controller.isQuorumLeader()) {
                requestBuilder.setTopic(topic)
                        .setLatencies(latencies)
                        .setPartitions(partitions)
                        .addAllPartitionMetadata(partitionRecords.stream().map(pr ->
                                PartitionMetadata
                                .newBuilder()
                                .setId(pr.getId())
                                .setLatency(pr.getLatency())
                                .setLeader(pr.getLeader())
                                .addAllReplicas(pr.getLatencies())
                                .build()).collect(Collectors.toSet()))
                        .build();
            }

            CreateTopicPrepareCommitResponse.Builder responseBuilder = CreateTopicPrepareCommitResponse.newBuilder();
            if (controller.isQuorumLeader()) {
                responseBuilder.setTopic(topic)
                        .setLatencies(latencies)
                        .setPartitions(partitions)
                        .addAllPartitionMetadata(partitionRecords.stream().map(pr ->
                                PartitionMetadata
                                        .newBuilder()
                                        .setId(pr.getId())
                                        .setLatency(pr.getLatency())
                                        .setLeader(pr.getLeader())
                                        .addAllReplicas(pr.getLatencies())
                                        .build()).collect(Collectors.toSet()))
                        .build();
            }
            CommitRecord<TopicRecord> commitRecord = new CommitRecord<>(record, ADD, requestBuilder.build(), responseBuilder.build());

            Set<PartitionRecord> cacheRecords = commitRecordCache.get(record.getName());
            if (isNull(cacheRecords)) {
                cacheRecords = new CopyOnWriteArraySet<>();
            }
            cacheRecords.addAll(partitionRecords);
            unCommitRecordCache.put(record.getName(), cacheRecords);

            return commitRecord;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create topic<" + record.getName() + ">");
        }
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
                String topic = record.getName();
                unCommitRecordCache.invalidate(topic);
                Set<PartitionRecord> partitionRecords = unCommitRecordCache.get(topic);
                if (isNull(partitionRecords)) {
                    partitionRecords = new CopyOnWriteArraySet<>();
                }
                partitionRecords.addAll(record.getPartitionRecords());
                commitRecordCache.put(record.getName(), partitionRecords);

                //TODO notify init log
                LogManager logManager = manager.getLogManager();
                logManager.initLog(topic, -1, -1);
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
