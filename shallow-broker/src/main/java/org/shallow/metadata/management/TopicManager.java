package org.shallow.metadata.management;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.netty.util.concurrent.Promise;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.shallow.internal.BrokerManager;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.invoke.ClientChannel;
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
import org.shallow.processor.ProcessCommand;
import org.shallow.proto.PartitionMetadata;
import org.shallow.proto.TopicMetadata;
import org.shallow.proto.elector.CreateTopicPrepareCommitRequest;
import org.shallow.proto.elector.CreateTopicPrepareCommitResponse;
import org.shallow.proto.elector.DeleteTopicPrepareCommitRequest;
import org.shallow.proto.elector.DeleteTopicPrepareCommitResponse;
import org.shallow.proto.server.QueryTopicInfoRequest;
import org.shallow.proto.server.QueryTopicInfoResponse;
import org.shallow.util.NetworkUtil;

import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.shallow.metadata.MetadataConstants.TOPICS;
import static org.shallow.util.JsonUtil.object2Json;

public class TopicManager extends AbstractSRaftLog<TopicRecord> {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(TopicManager.class);

    private final MappedFileApi api;
    private final DistributedAtomicInteger atomicValue;
    private final LoadingCache<String, Set<PartitionRecord>> topicCommitRecordCache;
    private final LoadingCache<String, Set<PartitionRecord>> topicUnCommitRecordCache;

    public TopicManager(Set<SocketAddress> quorumVoterAddresses, BrokerConfig config, BrokerManager manager) {
        super(quorumVoterAddresses, DefaultFixedChannelPoolFactory.INSTANCE.acquireChannelPool(), config, manager.getController());
        this.api = manager.getMappedFileApi();
        this.atomicValue = controller.getAtomicValue();

        this.topicCommitRecordCache = Caffeine.newBuilder()
                .expireAfterWrite(Long.MAX_VALUE, TimeUnit.DAYS)
                .expireAfterAccess(Long.MAX_VALUE, TimeUnit.DAYS)
                .build(new CacheLoader<>() {
                    @Override
                    public @Nullable Set<PartitionRecord> load(String key) throws Exception {
                        return syncFromQuorumLeader(key);
                    }
                });

        this.topicUnCommitRecordCache = Caffeine.newBuilder()
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

    public Set<PartitionRecord> getTopicInfo(String topic) {
        Set<PartitionRecord> partitionRecords = topicCommitRecordCache.get(topic);
        if (partitionRecords == null || partitionRecords.isEmpty()) {
            return null;
        }
        return partitionRecords;
    }

    private Set<PartitionRecord> syncFromQuorumLeader(String topic) {
        if (controller.isQuorumLeader()) {
            return null;
        }
        try {

            SocketAddress leader = controller.getMetadataLeader();
            ClientChannel clientChannel = pool.acquireHealthyOrNew(leader);

            QueryTopicInfoRequest request = QueryTopicInfoRequest
                    .newBuilder()
                    .setTopic(topic)
                    .build();
            Promise<QueryTopicInfoResponse> promise = NetworkUtil.newImmediatePromise();
            clientChannel.invoker().invoke(ProcessCommand.Server.FETCH_TOPIC_RECORD, config.getInvokeTimeMs(), promise, request, QueryTopicInfoResponse.class);

            QueryTopicInfoResponse response = promise.get(config.getInvokeTimeMs(), TimeUnit.MILLISECONDS);
            Map<String, TopicMetadata> topicsMap = response.getTopicsMap();

            if (topicsMap.isEmpty()) {
                if (logger.isWarnEnabled()) {
                    logger.warn("Query topic<{}> information is null");
                }
                return null;
            }

            Map<Integer, PartitionMetadata> partitionsMap = topicsMap
                    .entrySet()
                    .stream()
                    .iterator()
                    .next()
                    .getValue()
                    .getPartitionsMap();

            if (partitionsMap.isEmpty()) {
                if (logger.isWarnEnabled()) {
                    logger.warn("Query topic<{}> partition information is null");
                }
                return null;
            }

            return partitionsMap.values().stream()
                    .map(metadata -> new PartitionRecord(metadata.getId(), metadata.getLatency(), metadata.getLeader(), metadata.getReplicasList()))
                    .collect(Collectors.toCollection(CopyOnWriteArraySet::new));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
