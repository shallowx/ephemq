package org.shallow.internal.metadata;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.netty.util.concurrent.Promise;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.shallow.client.Client;
import org.shallow.client.internal.ClientChannel;
import org.shallow.client.internal.OperationInvoker;
import org.shallow.client.pool.ShallowChannelPool;
import org.shallow.common.logging.InternalLogger;
import org.shallow.common.logging.InternalLoggerFactory;
import org.shallow.common.meta.PartitionRecord;
import org.shallow.common.meta.TopicRecord;
import org.shallow.internal.BrokerManager;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.remote.processor.ProcessCommand;
import org.shallow.remote.proto.PartitionMetadata;
import org.shallow.remote.proto.TopicMetadata;
import org.shallow.remote.proto.server.CreateTopicRequest;
import org.shallow.remote.proto.server.CreateTopicResponse;
import org.shallow.remote.proto.server.QueryTopicInfoRequest;
import org.shallow.remote.proto.server.QueryTopicInfoResponse;
import org.shallow.remote.util.NetworkUtil;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.shallow.remote.processor.ProcessCommand.Nameserver.NEW_TOPIC;
import static org.shallow.remote.util.NetworkUtil.newImmediatePromise;

public class TopicPartitionRequestCache {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(TopicPartitionRequestCache.class);

    private final BrokerConfig config;
    private final Client internalClient;
    private final PartitionLeaderElector leaderElector;

    private final LoadingCache<String, Set<PartitionRecord>> cache;

    public TopicPartitionRequestCache(BrokerConfig config, BrokerManager manager) {
        this.config = config;
        this.internalClient = manager.getInternalClient();
        this.leaderElector = new PartitionLeaderElector(manager);

        this.cache = Caffeine.newBuilder().refreshAfterWrite(1, TimeUnit.MINUTES)
                .build(new CacheLoader<>() {
                    @Override
                    public @Nullable Set<PartitionRecord> load(String key) throws Exception {
                        try {
                            return loadFromNameserver(key);
                        } catch (Exception e) {
                            return null;
                        }
                    }
                });
    }

    public void createTopic(String topic, int partitions, int latencies, Promise<Void> promise) {
        try {
            Set<PartitionRecord> partitionRecords = cache.get(topic);
            if (partitionRecords != null && !partitionRecords.isEmpty()) {
                promise.tryFailure(new IllegalStateException(String.format("Topic already exists, and topic=%s", topic)));
                return;
            }

            OperationInvoker invoker = acquireInvokerByRandomClientChannel();

            leaderElector.elect();

            CreateTopicRequest request = CreateTopicRequest.newBuilder()
                    .setTopic(topic)
                    .setPartitions(partitions)
                    .build();

            Promise<Void> voidPromise = newImmediatePromise();
            voidPromise.addListener(future -> {
                if (future.isSuccess()) {
                    promise.trySuccess(null);
                } else {
                    promise.tryFailure(future.cause());
                }
            });

            invoker.invoke(NEW_TOPIC, config.getInvokeTimeMs(), voidPromise, request, CreateTopicResponse.class);

        } catch (Throwable t) {
            promise.tryFailure(t);
        }
    }

    public void delTopic(String topic, Promise<Void> promise) {
        try {
            Set<PartitionRecord> partitionRecords = cache.get(topic);
            if (partitionRecords == null || partitionRecords.isEmpty()) {
                promise.tryFailure(new IllegalStateException(String.format("Topic not exists, and topic=%s", topic)));
                return;
            }

        } catch (Throwable t) {
            promise.tryFailure(t);
        }
    }

    public Set<PartitionRecord> loadAll(List<String> topics) throws Exception {
        Set<PartitionRecord> records = new HashSet<>();
        if (topics == null || topics.isEmpty()) {
            Iterator<Set<PartitionRecord>> iterator = cache.asMap().values().stream().iterator();
            while (iterator.hasNext()) {
                records.addAll(iterator.next());
            }
        } else {
            for (String topic : topics) {
                Set<PartitionRecord> sets = this.cache.get(topic);
                if (sets == null || sets.isEmpty()) {
                    continue;
                }
                records.addAll(sets);
            }
        }
        return records;
    }

    private Set<PartitionRecord> loadFromNameserver(String topic) throws ExecutionException, InterruptedException {
        QueryTopicInfoRequest request = QueryTopicInfoRequest.newBuilder()
                .addAllTopic(List.of(topic))
                .build();

        OperationInvoker invoker = acquireInvokerByRandomClientChannel();
        Promise<QueryTopicInfoResponse> promise = NetworkUtil.newImmediatePromise();

        invoker.invoke(ProcessCommand.Server.FETCH_TOPIC_RECORD, config.getInvokeTimeMs(), promise, request, org.shallow.proto.server.QueryTopicInfoResponse.class);
        Map<String, TopicMetadata> records = promise.get().getTopicsMap();

        if (records.isEmpty()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Query topic record is empty");
            }
            return null;
        }

        return records.values().stream().map(metadata -> {
            Map<Integer, PartitionMetadata> partitionsMap = metadata.getPartitionsMap();

            Set<PartitionRecord> partitionRecords = new HashSet<>();
            for (Map.Entry<Integer, PartitionMetadata> entry : partitionsMap.entrySet()) {
                PartitionMetadata partitionMetadata = entry.getValue();

                PartitionRecord partitionRecord = PartitionRecord
                        .newBuilder()
                        .id(partitionMetadata.getId())
                        .latency(partitionMetadata.getLatency())
                        .leader(partitionMetadata.getLeader())
                        .latencies(partitionMetadata.getReplicasList())
                        .build();

                partitionRecords.add(partitionRecord);
            }
            return partitionRecords;
        }).findFirst().orElse(null);
    }

    private OperationInvoker acquireInvokerByRandomClientChannel() {
        ShallowChannelPool chanelPool = internalClient.getChanelPool();
        ClientChannel clientChannel = chanelPool.acquireWithRandomly();
        return clientChannel.invoker();
    }
}
