package org.shallow.metadata.snapshot;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.shallow.internal.BrokerManager;
import org.shallow.internal.ClientChannel;
import org.shallow.internal.atomic.DistributedAtomicInteger;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.log.LedgerManager;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.meta.PartitionRecord;
import org.shallow.meta.TopicRecord;
import org.shallow.metadata.MappingFileProcessor;
import org.shallow.metadata.MetadataManager;
import org.shallow.metadata.raft.LeaderElector;
import org.shallow.metadata.raft.RaftQuorumClient;
import org.shallow.metadata.raft.RaftVoteProcessor;
import org.shallow.pool.ShallowChannelPool;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;

import static org.shallow.util.NetworkUtil.newEventExecutorGroup;
import static org.shallow.util.NetworkUtil.newImmediatePromise;

public class TopicSnapshot {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(TopicSnapshot.class);

    private final MappingFileProcessor fileProcessor;
    private final BrokerConfig config;
    private final DistributedAtomicInteger atomicValue;
    private final LoadingCache<String, TopicRecord> topics;
    private final RaftVoteProcessor voteProcessor;
    private final PartitionLeaderElector leaderElector;
    private final LedgerManager ledgerManager;
    private final EventExecutor retryTaskExecutor;
    private final RaftQuorumClient client;
    private final ShallowChannelPool pool;

    public TopicSnapshot(MappingFileProcessor processor, BrokerConfig config, DistributedAtomicInteger atomicValue, RaftVoteProcessor voteProcessor, BrokerManager manager, RaftQuorumClient client) {
        this.fileProcessor = processor;
        this.config = config;
        this.atomicValue = atomicValue;
        this.ledgerManager = manager.getLedgerManager();
        this.voteProcessor = voteProcessor;
        this.leaderElector = new PartitionLeaderElector(voteProcessor);
        this.client = client;
        this.pool = client.getChanelPool();

        this.topics = Caffeine.newBuilder().build(new CacheLoader<>() {
            @Override
            public @Nullable TopicRecord load(String key) throws Exception {
                return applyFromNameServer(key);
            }
        });
        this.retryTaskExecutor = newEventExecutorGroup(1, "cluster-retry-task").next();
    }

    public void start() throws Exception {
        fill();
        retryTaskExecutor.schedule(this::fetchFromQuorumLeader, 1000, TimeUnit.MILLISECONDS);
    }

    private TopicRecord applyFromNameServer(String topic) {
        ClientChannel clientChannel = applyChannel();
        MetadataManager metadataManager = client.getMetadataManager();
        try {
            Map<String, TopicRecord> topicRecords = metadataManager.queryTopicRecord(clientChannel, List.of());
            if (topicRecords == null || topicRecords.isEmpty()) {
                return null;
            }
            return topicRecords.get(topic);
        } catch (Throwable ignored) {}

        return null;
    }

    public void fetchFromQuorumLeader() {
        LeaderElector elector = voteProcessor.getLeaderElector();
        if (config.isStandAlone() || elector.isLeader()) {
            return;
        }

        Promise<Void> promise = newImmediatePromise();
        promise.addListener((GenericFutureListener<Future<Void>>) future -> {
            if (!future.isSuccess()) {
                retryTaskExecutor.schedule(this::fetchFromQuorumLeader, 0, TimeUnit.MILLISECONDS);
            }
        });

        ClientChannel clientChannel = applyChannel();

        MetadataManager metadataManager = client.getMetadataManager();
        try {
            Map<String, TopicRecord> topicRecords = metadataManager.queryTopicRecord(clientChannel, List.of());
            if (topicRecords == null || topicRecords.isEmpty()) {
                promise.tryFailure(null);
                return;
            }

            Set<Map.Entry<String, TopicRecord>> entries = topicRecords.entrySet();
            for (Map.Entry<String, TopicRecord> entry : entries) {
                topics.put(entry.getKey(), entry.getValue());
            }
            promise.trySuccess(null);
        } catch (Throwable t) {
            promise.tryFailure(t);
        }
    }

    private ClientChannel applyChannel() {
        LeaderElector elector = voteProcessor.getLeaderElector();
        SocketAddress address = elector.getAddress();

        ClientChannel clientChannel = pool.acquireHealthyOrNew(address);
        return clientChannel;
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

        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        fileProcessor.write(gson.toJson(topics.asMap()));

    }

    public TopicRecord getRecord(String topic) {
        return topics.get(topic);
    }

    private void fill() throws IOException {
        String content = fileProcessor.read();

        Gson gson = new Gson();
        Map<String, TopicRecord> records = gson.fromJson(content, new TypeToken<Map<String, TopicRecord>>(){}.getType());
        if (records != null && !records.isEmpty()) {
            Set<Map.Entry<String, TopicRecord>> entries = records.entrySet();
            for (Map.Entry<String, TopicRecord> entry : entries) {
                String topic = entry.getKey();
                TopicRecord record = entry.getValue();

                topics.put(topic, record);
                Set<PartitionRecord> partitionRecords = record.getPartitionRecords();
                for (PartitionRecord partitionRecord : partitionRecords) {
                    ledgerManager.initLog(topic, partitionRecord.getId(), -1);
                }
            }
        }
    }
}
