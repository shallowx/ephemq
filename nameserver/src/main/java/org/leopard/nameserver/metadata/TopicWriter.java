package org.leopard.nameserver.metadata;

import io.netty.util.concurrent.Promise;
import org.leopard.common.logging.InternalLogger;
import org.leopard.common.logging.InternalLoggerFactory;
import org.leopard.common.metadata.PartitionRecord;
import org.leopard.remote.proto.PartitionMetadata;
import org.leopard.remote.proto.TopicMetadata;
import org.leopard.remote.proto.server.QueryTopicInfoResponse;

import javax.annotation.concurrent.ThreadSafe;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@ThreadSafe
public class TopicWriter {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(TopicWriter.class);

    private final Map<String, Map<String, Set<PartitionRecord>>> cache = new ConcurrentHashMap<>();

    public TopicWriter() {
    }

    public void create(String topic, String cluster, Set<PartitionRecord> partitionRecords, Promise<Void> promise) {
        synchronized (cache) {
            Map<String, Set<PartitionRecord>> clusterRecords = cache.get(cluster);
            if (clusterRecords == null) {
                clusterRecords = new ConcurrentHashMap<>();
            }

            if (clusterRecords.containsKey(topic)) {
                promise.tryFailure(new IllegalStateException(String.format("The topic is exists, topic = %s", topic)));
                return;
            }

            clusterRecords.put(topic, partitionRecords);
            cache.put(cluster, clusterRecords);
        }

        promise.trySuccess(null);
    }

    public void load(List<String> topics, String cluster, Promise<QueryTopicInfoResponse> promise) {
        Map<String, Set<PartitionRecord>> clusterRecords = cache.get(cluster);
        if (clusterRecords == null || clusterRecords.isEmpty()) {
            promise.trySuccess(null);
            return;
        }

        if (topics == null || topics.isEmpty()) {
            promise.trySuccess(null);
            return;
        }

        Map<String, TopicMetadata> results = new ConcurrentHashMap<>();
        for (String topic : topics) {
            Set<PartitionRecord> partitionRecords = clusterRecords.get(topic);
            if (partitionRecords.isEmpty()) {
                continue;
            }

            Map<Integer, PartitionMetadata> partitionMetadataMap = new ConcurrentHashMap<>();

            for (PartitionRecord partitionRecord : partitionRecords) {
                PartitionMetadata partitionMetadata = PartitionMetadata.newBuilder()
                        .addAllReplicas(partitionRecord.getLatencies())
                        .setLeader(partitionRecord.getLeader())
                        .setId(partitionRecord.getId())
                        .setLatency(partitionRecord.getLatency())
                        .build();

                partitionMetadataMap.put(partitionMetadata.getId(), partitionMetadata);
            }

            TopicMetadata topicMetadata = TopicMetadata.newBuilder()
                    .setName(cluster)
                    .putAllPartitions(partitionMetadataMap)
                    .build();

            results.put(topic, topicMetadata);
        }

        QueryTopicInfoResponse response = QueryTopicInfoResponse.newBuilder()
                .putAllTopics(results)
                .build();

        promise.trySuccess(response);
    }

    public void remove(String cluster, String topic, Promise<Void> promise) {
        try {
            synchronized (cache) {
                Map<String, Set<PartitionRecord>> clusterRecords = cache.get(cluster);
                if (clusterRecords != null && !clusterRecords.isEmpty()) {
                    clusterRecords.remove(topic);
                }
            }
            promise.trySuccess(null);
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("Remove topic from cluster failure, topic={} cluster={}", topic, cluster, t);
            }
            promise.tryFailure(t);
        }
    }
}
