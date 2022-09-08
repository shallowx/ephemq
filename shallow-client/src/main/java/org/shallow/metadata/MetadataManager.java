package org.shallow.metadata;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Promise;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.shallow.Client;
import org.shallow.ClientConfig;
import org.shallow.internal.ClientChannel;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.meta.NodeRecord;
import org.shallow.meta.PartitionRecord;
import org.shallow.meta.TopicRecord;
import org.shallow.pool.DefaultFixedChannelPoolFactory;
import org.shallow.pool.ShallowChannelPool;
import org.shallow.processor.ProcessCommand;
import org.shallow.proto.NodeMetadata;
import org.shallow.proto.PartitionMetadata;
import org.shallow.proto.TopicMetadata;
import org.shallow.proto.server.*;

import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.shallow.util.NetworkUtil.*;

public class MetadataManager implements ProcessCommand.Server {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MetadataManager.class);

    private final ShallowChannelPool pool;
    private final ClientConfig config;
    private final LoadingCache<String, MessageRouter> routers;
    private final EventExecutor scheduledMetadataTask;

    public MetadataManager(Client client) {
        this.pool = DefaultFixedChannelPoolFactory.INSTANCE.acquireChannelPool();
        this.config = client.getClientConfig();

        this.routers = Caffeine.newBuilder()
                .expireAfterAccess(config.getMetadataExpiredMs(), TimeUnit.MILLISECONDS)
                .expireAfterWrite(config.getMetadataExpiredMs(), TimeUnit.MILLISECONDS)
                .build(new CacheLoader<>() {
            @Override
            public @Nullable MessageRouter load(String key) throws Exception {
                TopicRecord topicRecord = queryTopicRecord(pool.acquireWithRandomly(), List.of(key)).get(key);
                Set<NodeRecord> nodeRecords = queryNodeRecord(pool.acquireWithRandomly());
                return assembleRouter(key, topicRecord, nodeRecords);
            }
        });
        this.scheduledMetadataTask = newEventExecutorGroup(1, "metadata-task").next();
    }

    public void start() throws Exception{
        scheduledMetadataTask.scheduleAtFixedRate(this::refreshMetadata, 5000,
                config.getRefreshMetadataIntervalMs(), TimeUnit.MILLISECONDS);
    }

    public Promise<CreateTopicResponse> createTopic(String topic, int partitions, int latency) {
        CreateTopicRequest request = CreateTopicRequest.newBuilder()
                .setTopic(topic)
                .setPartitions(partitions)
                .setLatencies(latency)
                .build();

        Promise<CreateTopicResponse> promise = newImmediatePromise();
        ClientChannel channel = pool.acquireWithRandomly();
        channel.invoker().invoke(CREATE_TOPIC, config.getInvokeExpiredMs(), promise, request, CreateTopicResponse.class);

        return promise;
    }

    public Promise<DelTopicResponse> delTopic(String topic) {
        DelTopicRequest request = DelTopicRequest.newBuilder().setTopic(topic).build();

        Promise<DelTopicResponse> promise = newImmediatePromise();

        ClientChannel channel = pool.acquireWithRandomly();
        channel.invoker().invoke(DELETE_TOPIC, config.getInvokeExpiredMs(), promise, request, DelTopicResponse.class);

        return promise;
    }

    public Map<String, TopicRecord> queryTopicRecord(ClientChannel clientChannel, List<String> topics) throws Exception {
        QueryTopicInfoRequest request = QueryTopicInfoRequest
                .newBuilder()
                .addAllTopic(topics)
                .build();

        Promise<QueryTopicInfoResponse> promise = newImmediatePromise();

        clientChannel.invoker().invoke(FETCH_TOPIC_RECORD, config.getInvokeExpiredMs(), promise, request, QueryTopicInfoResponse.class);
        Map<String, TopicMetadata> topicsMap = promise.get(config.getInvokeExpiredMs(), TimeUnit.MILLISECONDS).getTopicsMap();
        if (topicsMap.isEmpty()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Query topic record is empty");
            }
            return null;
        }

        return topicsMap.entrySet().stream().map(metadataEntry -> {
            String topic = metadataEntry.getKey();
            TopicMetadata metadata = metadataEntry.getValue();
            Map<Integer, PartitionMetadata> partitionsMap = metadata.getPartitionsMap();

            Set<PartitionRecord> partitionRecords = new HashSet<>();
            for (Map.Entry<Integer, PartitionMetadata> entry : partitionsMap.entrySet()) {
                PartitionMetadata partitionMetadata = entry.getValue();

                PartitionRecord partitionRecord = new PartitionRecord(partitionMetadata.getId(), partitionMetadata.getLatency(), partitionMetadata.getLeader(), partitionMetadata.getReplicasList());
                partitionRecords.add(partitionRecord);
            }
            return new TopicRecord(topic, partitionsMap.size(), partitionRecords);
        }).collect(Collectors.toMap(TopicRecord::getName, Function.identity()));
    }

    public Set<NodeRecord> queryNodeRecord(ClientChannel clientChannel) throws Exception {
        QueryClusterNodeRequest request = QueryClusterNodeRequest
                .newBuilder()
                .build();

        Promise<QueryClusterNodeResponse> promise = newImmediatePromise();
        clientChannel.invoker().invoke(FETCH_CLUSTER_RECORD, config.getInvokeExpiredMs(), promise, request, QueryClusterNodeResponse.class);
        List<NodeMetadata> nodes = promise.get(config.getInvokeExpiredMs(), TimeUnit.MILLISECONDS).getNodesList();
        if (nodes.isEmpty()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Query node record is empty");
            }
            return null;
        }

        return nodes.stream()
                 .map(nodeMetadata -> new NodeRecord(nodeMetadata.getCluster(), nodeMetadata.getName(), nodeMetadata.getState(), switchSocketAddress(nodeMetadata.getHost(), nodeMetadata.getPort())))
                 .collect(Collectors.toSet());
    }

    public MessageRouter queryRouter(String topic) {
        MessageRouter messageRouter = routers.get(topic);
        if (null == messageRouter) {
            ClientChannel clientChannel = pool.acquireWithRandomly();
            try {
                Set<NodeRecord> nodeRecords = queryNodeRecord(clientChannel);
                if (nodeRecords == null || nodeRecords.isEmpty()) {
                    return null;
                }

                Map<String, TopicRecord> topicRecords = queryTopicRecord(clientChannel, List.of(topic));
                if (topicRecords == null || topicRecords.isEmpty()) {
                    return null;
                }
                TopicRecord topicRecord = topicRecords.get(topic);
                MessageRouter router = assembleRouter(topic, topicRecord, nodeRecords);
                routers.put(topic, router);
                return router;
            } catch (Throwable t) {
                if (logger.isErrorEnabled()) {
                    logger.error("Failed to query topic<{}> router, error:{}", topic, t);
                }
            }
        }
        return messageRouter;
    }

    private MessageRouter assembleRouter(String topic, TopicRecord topicRecord, Set<NodeRecord> nodeRecords) {
        Set<PartitionRecord> partitionRecords = topicRecord.getPartitionRecords();
        if (null == partitionRecords || partitionRecords.isEmpty()) {
            return null;
        }

        Map<Integer, MessageRoutingHolder> holders = new ConcurrentHashMap<>();
        for (PartitionRecord partitionRecord : partitionRecords) {
            int ledgerId = partitionRecord.getLatency();
            int partition = partitionRecord.getId();

            NodeRecord leaderNode = nodeRecords.stream()
                    .filter(nodeRecord -> nodeRecord.getName().equals(partitionRecord.getLeader()))
                    .findFirst()
                    .orElse(null);

            if (null == leaderNode) {
                continue;
            }
            SocketAddress leader = leaderNode.getSocketAddress();

            List<String> latencyNodes = partitionRecord.getLatencies();
            Set<SocketAddress> latencies = nodeRecords.stream()
                    .filter(nodeRecord -> latencyNodes.contains(nodeRecord.getName()))
                    .map(NodeRecord::getSocketAddress)
                    .collect(Collectors.toSet());

            MessageRoutingHolder routeHolder = new MessageRoutingHolder(topic, ledgerId, partition, leader, latencies);
            holders.put(ledgerId, routeHolder);
        }
        return new MessageRouter(topic, holders);
    }

    private void refreshMetadata() {
        try {
            List<String> topics = new ArrayList<>(routers.asMap().keySet());
            if (topics.isEmpty()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Router record is empty");
                }
                return;
            }

            Map<String, TopicRecord> topicRecords = queryTopicRecord(pool.acquireWithRandomly(), topics);
            if (topicRecords.isEmpty()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Topic record is empty");
                }
                return;
            }

            Set<NodeRecord> nodeRecords = queryNodeRecord(pool.acquireWithRandomly());
            if (null == nodeRecords) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Node record is empty");
                }
                return;
            }

            for (String topic : topics) {
                TopicRecord topicRecord = topicRecords.get(topic);
                MessageRouter messageRouter = assembleRouter(topic, topicRecord, nodeRecords);
                if (null == messageRouter) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Topic<{}> message router is  null", topic);
                    }
                    continue;
                }

                if (logger.isDebugEnabled()) {
                    logger.debug("Message router:{}", messageRouter);
                }

                routers.put(topic, messageRouter);
            }
        } catch (Exception e) {
          if (logger.isErrorEnabled()) {
              logger.error("Failed to refresh metadata, cause:{}", e);
          }
        }
    }
}
