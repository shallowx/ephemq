package org.ostara.client.internal.metadata;

import static org.ostara.remote.util.NetworkUtils.newEventExecutorGroup;
import static org.ostara.remote.util.NetworkUtils.newImmediatePromise;
import static org.ostara.remote.util.NetworkUtils.switchSocketAddress;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.ostara.client.internal.Client;
import org.ostara.client.internal.ClientChannel;
import org.ostara.client.internal.ClientConfig;
import org.ostara.client.internal.pool.DefaultFixedChannelPoolFactory;
import org.ostara.client.internal.pool.ShallowChannelPool;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.common.metadata.Node;
import org.ostara.common.metadata.Partition;
import org.ostara.common.metadata.Topic;
import org.ostara.remote.processor.ProcessCommand;
import org.ostara.remote.proto.NodeMetadata;
import org.ostara.remote.proto.PartitionMetadata;
import org.ostara.remote.proto.TopicMetadata;
import org.ostara.remote.proto.server.CreateTopicRequest;
import org.ostara.remote.proto.server.CreateTopicResponse;
import org.ostara.remote.proto.server.DelTopicRequest;
import org.ostara.remote.proto.server.DelTopicResponse;
import org.ostara.remote.proto.server.QueryClusterNodeRequest;
import org.ostara.remote.proto.server.QueryClusterNodeResponse;
import org.ostara.remote.proto.server.QueryTopicInfoRequest;
import org.ostara.remote.proto.server.QueryTopicInfoResponse;

public class MetadataWriter implements ProcessCommand.Server {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(MetadataWriter.class);

    private final ShallowChannelPool pool;
    private final ClientConfig config;
    private final LoadingCache<String, MessageRouter> routers;
    private final EventExecutor scheduledMetadataTask;

    public MetadataWriter(Client client) {
        this.pool = DefaultFixedChannelPoolFactory.INSTANCE.accessChannelPool();
        this.config = client.getClientConfig();

        this.routers = Caffeine.newBuilder()
                .expireAfterAccess(config.getMetadataExpiredMs(), TimeUnit.MILLISECONDS)
                .expireAfterWrite(config.getMetadataExpiredMs(), TimeUnit.MILLISECONDS)
                .build(new CacheLoader<>() {
                    @Override
                    public @Nullable MessageRouter load(String key) throws Exception {
                        Topic topicRecord = queryTopicRecord(pool.acquireWithRandomly(), List.of(key)).get(key);
                        Set<Node> nodeRecords = queryNodeRecord(pool.acquireWithRandomly());
                        return assembleRouter(key, topicRecord, nodeRecords);
                    }
                });
        this.scheduledMetadataTask = newEventExecutorGroup(1, "metadata-task").next();
    }

    public void start() throws Exception {
        scheduledMetadataTask.scheduleAtFixedRate(this::refreshMetadata, 5000,
                config.getRefreshMetadataIntervalMs(), TimeUnit.MILLISECONDS);
    }

    public Promise<CreateTopicResponse> createTopic(String topic, int partitionLimit, int replicateLimit) {
        CreateTopicRequest request = CreateTopicRequest.newBuilder()
                .setTopic(topic)
                .setPartitionLimit(partitionLimit)
                .setReplicateLimit(replicateLimit)
                .build();

        Promise<CreateTopicResponse> promise = newImmediatePromise();
        ClientChannel channel = pool.acquireWithRandomly();
        channel.invoker()
                .invoke(CREATE_TOPIC, config.getInvokeExpiredMs(), promise, request, CreateTopicResponse.class);

        return promise;
    }

    public Promise<DelTopicResponse> delTopic(String topic) {
        DelTopicRequest request = DelTopicRequest.newBuilder().setTopic(topic).build();

        Promise<DelTopicResponse> promise = newImmediatePromise();

        ClientChannel channel = pool.acquireWithRandomly();
        channel.invoker().invoke(DELETE_TOPIC, config.getInvokeExpiredMs(), promise, request, DelTopicResponse.class);

        return promise;
    }

    public Map<String, Topic> queryTopicRecord(ClientChannel clientChannel, List<String> topics) throws Exception {
        QueryTopicInfoRequest request = QueryTopicInfoRequest
                .newBuilder()
                .addAllTopic(topics)
                .build();

        Promise<QueryTopicInfoResponse> promise = newImmediatePromise();

        clientChannel.invoker().invoke(FETCH_TOPIC_RECORD, config.getInvokeExpiredMs(), promise, request,
                QueryTopicInfoResponse.class);
        Map<String, TopicMetadata> topicsMap =
                promise.get(config.getInvokeExpiredMs(), TimeUnit.MILLISECONDS).getTopicsMap();
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

            Set<Partition> partitions = new HashSet<>();
            for (Map.Entry<Integer, PartitionMetadata> entry : partitionsMap.entrySet()) {
                PartitionMetadata partitionMetadata = entry.getValue();

                Partition partitionRecord = Partition
                        .newBuilder()
                        .id(partitionMetadata.getId())
                        .ledgerId(partitionMetadata.getLatency())
                        .leader(partitionMetadata.getLeader())
                        .replicates(partitionMetadata.getReplicasList())
                        .build();

                partitions.add(partitionRecord);
            }
            return Topic
                    .newBuilder()
                    .name(topic)
                    .partitions(partitions)
                    .partitionLimit(partitionsMap.size())
                    .build();
        }).collect(Collectors.toMap(Topic::getName, Function.identity()));
    }

    public Set<Node> queryNodeRecord(ClientChannel clientChannel) throws Exception {
        QueryClusterNodeRequest request = QueryClusterNodeRequest
                .newBuilder()
                .build();

        Promise<QueryClusterNodeResponse> promise = newImmediatePromise();
        clientChannel.invoker().invoke(FETCH_CLUSTER_RECORD, config.getInvokeExpiredMs(), promise, request,
                QueryClusterNodeResponse.class);
        List<NodeMetadata> nodes = promise.get(config.getInvokeExpiredMs(), TimeUnit.MILLISECONDS).getNodesList();
        if (nodes.isEmpty()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Query node record is empty");
            }
            return null;
        }

        return nodes.stream()
                .map(nodeMetadata -> Node
                        .newBuilder()
                        .cluster(nodeMetadata.getCluster())
                        .name(nodeMetadata.getName())
                        .state(nodeMetadata.getState())
                        .lastKeepLiveTime(nodeMetadata.getLastKeepLiveTime())
                        .socketAddress(switchSocketAddress(nodeMetadata.getHost(), nodeMetadata.getPort()))
                        .build())
                .collect(Collectors.toSet());
    }

    public Map<String, MessageRouter> getWholesRoutes() {
        return routers.asMap();
    }

    public MessageRouter queryRouter(String topic) {
        MessageRouter messageRouter = routers.get(topic);
        if (null == messageRouter) {
            ClientChannel clientChannel = pool.acquireWithRandomly();
            try {
                Set<Node> nodeRecords = queryNodeRecord(clientChannel);
                if (nodeRecords == null || nodeRecords.isEmpty()) {
                    return null;
                }

                Map<String, Topic> topicRecords = queryTopicRecord(clientChannel, List.of(topic));
                if (topicRecords == null || topicRecords.isEmpty()) {
                    return null;
                }
                Topic topicRecord = topicRecords.get(topic);
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

    private MessageRouter assembleRouter(String topic, Topic topicRecord, Set<Node> nodes) {
        Set<Partition> partitions = topicRecord.getPartitions();
        if (null == partitions || partitions.isEmpty()) {
            return null;
        }

        Map<Integer, MessageRoutingHolder> holders = new ConcurrentHashMap<>();
        for (Partition partition : partitions) {
            int ledgerId = partition.getLedgerId();
            int partitionId = partition.getId();

            Node leaderNode = nodes.stream()
                    .filter(nodeRecord -> nodeRecord.getName().equals(partition.getLeader()))
                    .findFirst()
                    .orElse(null);

            if (null == leaderNode) {
                continue;
            }
            SocketAddress leader = leaderNode.getSocketAddress();

            List<String> replicates = partition.getReplicates();
            Set<SocketAddress> replicatesAddr = nodes.stream()
                    .filter(nodeRecord -> replicates.contains(nodeRecord.getName()))
                    .map(Node::getSocketAddress)
                    .collect(Collectors.toSet());

            MessageRoutingHolder routeHolder =
                    new MessageRoutingHolder(topic, ledgerId, partitionId, leader, replicatesAddr);
            holders.put(ledgerId, routeHolder);
        }
        return new MessageRouter(topic, holders);
    }

    public void refreshMetadata() {
        try {
            List<String> topics = new ArrayList<>(routers.asMap().keySet());
            if (topics.isEmpty()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Router record is empty");
                }
                return;
            }

            Map<String, Topic> topicRecords = queryTopicRecord(pool.acquireWithRandomly(), topics);
            if (topicRecords.isEmpty()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Topic record is empty");
                }
                return;
            }

            Set<Node> nodeRecords = queryNodeRecord(pool.acquireWithRandomly());
            if (null == nodeRecords) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Node record is empty");
                }
                return;
            }

            for (String topic : topics) {
                Topic topicRecord = topicRecords.get(topic);
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

    public synchronized void shutdownGracefully(Supplier<Void> supplier) {
        if (scheduledMetadataTask == null || scheduledMetadataTask.isShutdown()
                || scheduledMetadataTask.isTerminated()) {
            return;
        }

        Future<?> future = scheduledMetadataTask.shutdownGracefully();
        future.addListener(f -> supplier.get());
    }
}
