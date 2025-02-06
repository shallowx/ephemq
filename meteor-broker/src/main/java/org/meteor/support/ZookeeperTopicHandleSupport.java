package org.meteor.support;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.netty.util.concurrent.EventExecutor;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.DeleteBuilder;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicInteger;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.RetryOneTime;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.common.message.*;
import org.meteor.config.CommonConfig;
import org.meteor.config.SegmentConfig;
import org.meteor.config.ServerConfig;
import org.meteor.config.ZookeeperConfig;
import org.meteor.exception.TopicHandleException;
import org.meteor.internal.CorrelationIdConstants;
import org.meteor.internal.ZookeeperClientFactory;
import org.meteor.ledger.Log;
import org.meteor.ledger.LogHandler;
import org.meteor.listener.TopicListener;

import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The ZookeeperTopicHandleSupport class provides methods for managing topics and
 * their partitions, specifically interfacing with Zookeeper for coordination.
 */
public class ZookeeperTopicHandleSupport implements TopicHandleSupport {
    /**
     * A constant key used to represent all topics in the ZookeeperTopicHandleSupport.
     * This key is used internally to handle operations that apply to all topics.
     */
    protected static final String ALL_TOPIC_KEY = "ALL-TOPIC";
    /**
     *
     */
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ZookeeperClusterManager.class);
    /**
     * A regular expression pattern for matching Zookeeper paths that represent topic partitions.
     * <p>
     * The pattern matches paths of the form:
     * "/brokers/topics/[topic-name]/partitions/[partition-number]"
     * <p>
     * - Topic names can consist of word characters (alphanumeric), hyphens, and hash symbols.
     * - Partition numbers are numerical and represented by one or more digits.
     */
    private static final String TOPIC_PARTITION_REGEX = "^/brokers/topics/[\\w\\-#]+/partitions/\\d+$";
    /**
     * Regular expression pattern to match topic paths in Zookeeper.
     * This pattern ensures the path starts with "/brokers/topics/" followed by
     * one or more word characters, hyphens, or hash symbols.
     */
    private static final String TOPIC_REGEX = "^/brokers/topics/[\\w\\-#]+$";
    /**
     * A map that associates a partition identifier (represented as an Integer) with a
     * corresponding ZookeeperPartitionElector instance. This map is protected and final,
     * indicating that it can only be modified within the class it is declared in and
     * cannot be reassigned to a different instance after initialization.
     */
    protected final Map<Integer, ZookeeperPartitionElector> leaderElectorMap = new ConcurrentHashMap<>();
    /**
     * A list for storing TopicListener instances. These listeners are notified
     * of various events that occur on topic partitions, such as initialization,
     * destruction, leader assignments, and other topic-related activities.
     */
    protected final List<TopicListener> listeners = new ObjectArrayList<>();
    /**
     * A consistent hashing ring used to manage the distribution of topics
     * across different nodes in a cluster. The consistent hashing mechanism
     * ensures efficient and balanced topic partitioning, aiding in the
     * scalability and fault tolerance of the system.
     */
    private final ConsistentHashingRing hashingRing;
    /**
     * Holds the common configuration settings needed for managing and
     * handling topics within the Zookeeper-based architecture.
     * This configuration ensures consistency and standardization
     * across different parts of the application.
     */
    protected CommonConfig commonConfiguration;
    /**
     * Holds the configuration settings related to segments within the Zookeeper-based topic handling support framework.
     * It is used to manage and retrieve various segment configuration parameters like rolling size, retain limit, and retention time.
     */
    protected SegmentConfig segmentConfiguration;
    /**
     * The CuratorFramework client used for interacting with ZooKeeper.
     * This client is responsible for handling all communications with the ZooKeeper ensemble,
     * including operations such as creating, updating, and deleting z-nodes.
     * It also manages the connection state and handles reconnection logic.
     */
    protected CuratorFramework client;
    /**
     * A cache used to store and manage topic-related data.
     * The cache interacts with Zookeeper to keep topic information updated and consistent.
     */
    protected CuratorCache cache;
    /**
     * Manages and provides the support structure for topic participants within
     * the Zookeeper-based topic handling mechanism. This includes participant
     * management, status tracking, and coordination of operations involving
     * multiple participants.
     */
    protected ParticipantSupport participantSupport;
    /**
     * A distributed atomic integer used to generate unique topic IDs.
     * This variable is protected and is part of the ZookeeperTopicHandleSupport class which manages topic operations.
     * It ensures that topic IDs are consistent and unique across distributed systems.
     */
    protected DistributedAtomicInteger topicIdGenerator;
    /**
     * This variable is responsible for generating unique ledger IDs in a distributed manner.
     * It leverages a DistributedAtomicInteger to maintain a counter which ensures that each ledger ID generated is unique.
     * This is crucial for maintaining consistency and avoiding conflicts in a distributed system.
     */
    protected DistributedAtomicInteger ledgerIdGenerator;
    /**
     * The manager responsible for overseeing server components and their interactions
     * within the ZookeeperTopicHandleSupport context.
     * <p>
     * This instance is crucial for managing the start and shutdown of operations, as well as
     * providing access to various support components needed for handling topics, clusters, and more.
     */
    protected Manager manager;
    /**
     * A protected cache that loads and stores sets of PartitionInfo objects for each topic.
     * <p>
     * The topicCache is used to cache partition information retrieved from Zookeeper,
     * to reduce the number of direct requests made to the Zookeeper service. The cache
     * uses a LoadingCache, which allows values to be automatically loaded when they are
     * not present in the cache.
     * <p>
     * Each entry in the cache is keyed by the topic name (String) and holds a set of PartitionInfo
     * objects, which provide detailed information about each partition of the topic.
     * <p>
     * This cache helps in optimizing the performance of topic partition lookups within
     * the ZookeeperTopicHandleSupport class by minimizing frequent remote calls.
     */
    protected LoadingCache<String, Set<PartitionInfo>> topicCache;
    /**
     * A protected cache that maps topic names to their respective sets of topic identifiers.
     * The cache is designed to lazily load and store sets of topic names upon request,
     * using a LoadingCache implementation.
     * <p>
     * Used primarily to optimize the retrieval and storage of topic names within the
     * Zookeeper-based topic handling mechanism.
     */
    protected LoadingCache<String, Set<String>> topicNamesCache;
    /**
     * Configuration settings for connecting to and managing Zookeeper.
     * <p>
     * This variable holds an instance of {@link ZookeeperConfig} which encapsulates
     * the necessary configurations needed to interact with Zookeeper.
     */
    private ZookeeperConfig zookeeperConfiguration;

    /**
     * Default constructor for the ZookeeperTopicHandleSupport class.
     * This constructor initializes a new instance with default settings.
     */
    public ZookeeperTopicHandleSupport() {
        this.hashingRing = null;
    }

    /**
     * Initializes the ZookeeperTopicHandleSupport with the provided configurations and manager.
     * It sets up the necessary caches, Zookeeper client, and distributed atomic integers
     * for topic and ledger ID generation.
     *
     * @param config The server configuration containing common, segment, and Zookeeper configs.
     * @param manager The manager responsible for handling participants and topic operations.
     * @param hashingRing The consistent hashing ring used for distributing topics among nodes.
     */
    public ZookeeperTopicHandleSupport(ServerConfig config, Manager manager, ConsistentHashingRing hashingRing) {
        this.commonConfiguration = config.getCommonConfig();
        this.segmentConfiguration = config.getSegmentConfig();
        this.zookeeperConfiguration = config.getZookeeperConfig();
        this.manager = manager;
        this.hashingRing = hashingRing;
        this.participantSupport = new ParticipantSupport(manager);
        this.client = ZookeeperClientFactory.getReadyClient(config.getZookeeperConfig(), commonConfiguration.getClusterName());
        this.topicIdGenerator = new DistributedAtomicInteger(this.client, CorrelationIdConstants.TOPIC_ID_COUNTER, new RetryOneTime(100));
        this.ledgerIdGenerator = new DistributedAtomicInteger(this.client, CorrelationIdConstants.LEDGER_ID_COUNTER, new RetryOneTime(100));
        this.topicCache = Caffeine.newBuilder().refreshAfterWrite(1, TimeUnit.MINUTES)
                .build(new CacheLoader<>() {
                    @Override
                    public @Nullable Set<PartitionInfo> load(String topic) throws Exception {
                        try {
                            return getTopicInfoFromZookeeper(topic);
                        } catch (IllegalArgumentException e) {
                            if (logger.isDebugEnabled()) {
                                logger.debug("Get topic[{}] info from zookeeper failed", topic, e.getMessage(), e);
                            }
                            return null;
                        }
                    }
                });

        this.topicNamesCache = Caffeine.newBuilder().refreshAfterWrite(1, TimeUnit.MINUTES)
                .build(new CacheLoader<>() {
                    @Nonnull
                    @Override
                    public Set<String> load(String s) throws Exception {
                        return getAllTopicsFromZookeeper();
                    }
                });
    }

    /**
     * Refreshes the partition information for a specific topic based on the provided assignment information.
     *
     * @param topic The topic for which the partition information needs to be refreshed.
     * @param stat The Stat object containing the version information for the node.
     * @param assignment The TopicAssignment object containing detailed information about the partition such as partition ID, leader, replicas, ledger ID, epoch, and configuration
     * .
     */
    private void refreshPartitionInfo(String topic, Stat stat, TopicAssignment assignment) {
        String path = String.format(PathConstants.BROKER_TOPIC_ID, topic);
        try {
            byte[] bytes = client.getData().forPath(path);
            int topicId = Integer.parseInt(new String(bytes, StandardCharsets.UTF_8));
            int partition = assignment.getPartition();
            String leader = assignment.getLeader();
            Set<String> replicas = assignment.getReplicas();
            int ledgerId = assignment.getLedgerId();
            int epoch = assignment.getEpoch();
            TopicConfig topicConfig = assignment.getConfig();
            PartitionInfo partitionInfo = new PartitionInfo(topic, topicId, partition, ledgerId, epoch, leader, replicas, topicConfig, stat.getVersion());
            Set<PartitionInfo> partitionInfos = topicCache.get(topic);
            if (partitionInfos == null) {
                partitionInfos = new HashSet<>();
            }

            partitionInfos.remove(partitionInfo);
            partitionInfos.add(partitionInfo);
            topicCache.put(topic, partitionInfos);
        } catch (Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Refresh partition error", e.getMessage(), e);
            }
            topicCache.invalidate(topic);
        }
    }

    /**
     * Fetches the partition information of a topic from Zookeeper.
     *
     * @param topic The name of the topic to retrieve partition information for.
     * @return A set containing partition information for the specified topic, or null if the topic does not exist.
     * @throws Exception if an error occurs while interacting with Zookeeper.
     */
    private Set<PartitionInfo> getTopicInfoFromZookeeper(String topic) throws Exception {
        String partitionsPath = String.format(PathConstants.BROKER_TOPIC_PARTITIONS, topic);
        String topicIdPath = String.format(PathConstants.BROKER_TOPIC_ID, topic);
        try {
            byte[] bytes = client.getData().forPath(topicIdPath);
            int topicId = Integer.parseInt(new String(bytes, StandardCharsets.UTF_8));
            List<String> partitions = client.getChildren().forPath(partitionsPath);
            if (partitions == null || partitions.isEmpty()) {
                return null;
            }

            Set<PartitionInfo> partitionInfos = new ObjectOpenHashSet<>();
            for (String nodeName : partitions) {
                int partition = Integer.parseInt(nodeName);
                String partitionPath = String.format(PathConstants.BROKER_TOPIC_PARTITION, topic, partition);
                Stat stat = new Stat();
                bytes = client.getData().storingStatIn(stat).forPath(partitionPath);
                TopicAssignment assignment = SerializeFeatureSupport.deserialize(bytes, TopicAssignment.class);
                String leader = assignment.getLeader();
                Set<String> replicas = assignment.getReplicas();
                int ledgerId = assignment.getLedgerId();
                int epoch = assignment.getEpoch();
                TopicConfig topicConfig = assignment.getConfig();
                PartitionInfo partitionInfo = new PartitionInfo(topic, topicId, partition, ledgerId, epoch, leader, replicas, topicConfig, stat.getVersion());
                partitionInfos.add(partitionInfo);
            }
            return partitionInfos;
        } catch (Exception e) {
            throw new TopicHandleException(String.format("Topic[%s] dose not exist", topic));
        }
    }

    /**
     * Starts the participant support and initializes the cache for managing topic and partition assignments.
     * This method sets up various listeners to handle add, remove, and update events for topics and partitions.
     *
     * @throws Exception if an error occurs during the start process
     */
    @Override
    public void start() throws Exception {
        participantSupport.start();
        cache = CuratorCache.build(client, PathConstants.BROKERS_TOPICS);
        CuratorCacheListener listener = CuratorCacheListener.builder().forTreeCache(client, new TreeCacheListener() {
            final Pattern TOPIC_PARTITION = Pattern.compile(TOPIC_PARTITION_REGEX);
            final Pattern TOPIC = Pattern.compile(TOPIC_REGEX);

            /**
             * Handles child events triggered by changes in the Zookeeper tree nodes.
             * This method processes different types of events such as NODE_ADDED, NODE_REMOVED, and NODE_UPDATED
             * by invoking the corresponding handler methods.
             *
             * @param curatorFramework the Curator framework instance managing the Zookeeper connection.
             * @param event the event received from the Zookeeper tree cache.
             * @throws Exception if an error occurs while handling the event.
             */
            @Override
            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent event) throws Exception {
                TreeCacheEvent.Type type = event.getType();
                switch (type) {
                    case NODE_ADDED -> handleAdd(event);
                    case NODE_REMOVED -> handleRemove(event);
                    case NODE_UPDATED -> handleUpdated(event);
                }
            }

            /**
             * Matches the given string against the specified regular expression pattern.
             *
             * @param origin the string to match against the pattern
             * @param pattern the regular expression pattern to use for matching; if null, the method returns true
             * @return true if the string matches the pattern, false otherwise
             */
            boolean regularExpressionMatcher(String origin, Pattern pattern) {
                if (pattern == null) {
                    return true;
                }
                Matcher matcher = pattern.matcher(origin);
                return matcher.matches();
            }

            /**
             * Checks if the given path corresponds to a topic partition node based on the
             * pre-defined regular expression pattern for topic partitions.
             *
             * @param path the path to be checked.
             * @return true if the path matches the topic partition pattern, false otherwise.
             */
            private boolean isTopicPartitionNode(String path) {
                return regularExpressionMatcher(path, TOPIC_PARTITION);
            }

            /**
             * Determines if the specified path corresponds to a topic node.
             *
             * @param path the path to be checked.
             * @return true if the path matches the topic node pattern, false otherwise.
             */
            private boolean isTopicNode(String path) {
                return regularExpressionMatcher(path, TOPIC);
            }

            /**
             * Handles updates to topic partition nodes based on the events triggered by changes in the Zookeeper tree nodes.
             * This method processes the event data to handle changes in topic partitions and update the listeners accordingly.
             *
             * @param event the event received from the Zookeeper tree cache.
             * @throws Exception if an error occurs while handling the update event.
             */
            private void handleUpdated(TreeCacheEvent event) throws Exception {
                ChildData data = event.getData();
                ChildData oldData = event.getOldData();
                String path = data.getPath();
                if (isTopicPartitionNode(path)) {
                    byte[] nodeData = data.getData();
                    int version = data.getStat().getVersion();
                    TopicAssignment assignment = SerializeFeatureSupport.deserialize(nodeData, TopicAssignment.class);
                    assignment.setVersion(version);

                    Set<String> replicas = assignment.getReplicas();
                    byte[] oldNodeData = oldData.getData();
                    int oldVersion = oldData.getStat().getVersion();
                    TopicAssignment oldAssignment =
                            SerializeFeatureSupport.deserialize(oldNodeData, TopicAssignment.class);
                    oldAssignment.setVersion(oldVersion);

                    TopicPartition topicPartition = new TopicPartition(assignment.getTopic(), assignment.getPartition());
                    refreshPartitionInfo(topicPartition.topic(), data.getStat(), assignment);
                    for (TopicListener topicListener : listeners) {
                        topicListener.onPartitionChanged(topicPartition, oldAssignment, assignment);
                    }

                    if (oldAssignment.getTransitionalLeader() != null && assignment.getTransitionalLeader() == null) {
                        if (commonConfiguration.getServerId().equals(oldAssignment.getTransitionalLeader())) {
                            destroyTopicPartitionAsync(topicPartition, assignment.getLedgerId());
                        }
                    }

                    Set<String> oldReplicas = oldAssignment.getReplicas();
                    if (replicas.equals(oldReplicas)) {
                        return;
                    }

                    Set<String> reducedReplicas = new HashSet<>(oldReplicas);
                    reducedReplicas.removeAll(replicas);
                    for (String id : reducedReplicas) {
                        if (id.equals(commonConfiguration.getServerId())) {
                            String transitionalLeader = assignment.getTransitionalLeader();
                            if (id.equals(transitionalLeader)) {
                                continue;
                            }
                            destroyTopicPartitionAsync(topicPartition, assignment.getLedgerId());
                        }
                    }
                    Set<String> addReplicas = new HashSet<>(oldReplicas);
                    addReplicas.removeAll(oldReplicas);
                    for (String id : addReplicas) {
                        if (id.equals(commonConfiguration.getServerId())) {
                            initTopicPartitionAsync(topicPartition, assignment.getLedgerId(), assignment.getEpoch(), assignment.getConfig());
                        }
                    }

                }
            }

            /**
             * Handles the removal of a node from the Zookeeper tree cache. This method processes the removal
             * of both topic nodes and topic partition nodes accordingly.
             *
             * @param event the event received from the Zookeeper tree cache, containing data about the removed node
             * @throws Exception if an error occurs while handling the removal event
             */
            private void handleRemove(TreeCacheEvent event) throws Exception {
                ChildData data = event.getData();
                String path = data.getPath();
                if (isTopicNode(path)) {
                    for (TopicListener topicListener : listeners) {
                        String topic = path.substring(path.lastIndexOf("/") + 1);
                        topicListener.onTopicDeleted(topic);
                    }

                    topicNamesCache.invalidateAll();
                    return;
                }

                if (isTopicPartitionNode(path)) {
                    byte[] nodeData = data.getData();
                    TopicAssignment assignment = SerializeFeatureSupport.deserialize(nodeData, TopicAssignment.class);
                    String topic = assignment.getTopic();
                    int partition = assignment.getPartition();
                    Set<String> replicas = assignment.getReplicas();
                    TopicPartition topicPartition = new TopicPartition(topic, partition);
                    if (replicas.contains(commonConfiguration.getServerId())) {
                        destroyTopicPartitionAsync(topicPartition, assignment.getLedgerId());
                    }
                    topicCache.invalidate(topicPartition.topic());
                }
            }

            /**
             * Handles the addition of a new node in the tree cache.
             * This method processes the event based on whether the new node is a topic node or a topic partition node.
             *
             * @param event the event received from the Zookeeper tree cache, containing details about the added node.
             * @throws Exception if an error occurs during the handling of the event.
             */
            private void handleAdd(TreeCacheEvent event) throws Exception {
                ChildData data = event.getData();
                String path = data.getPath();
                if (isTopicNode(path)) {
                    for (TopicListener topicListener : listeners) {
                        String topic = path.substring(path.lastIndexOf("/") + 1);
                        topicListener.onTopicCreated(topic);
                    }

                    topicNamesCache.invalidateAll();
                    return;
                }

                if (isTopicPartitionNode(path)) {
                    byte[] nodeData = data.getData();
                    TopicAssignment assignment = SerializeFeatureSupport.deserialize(nodeData, TopicAssignment.class);
                    String topic = assignment.getTopic();
                    int partition = assignment.getPartition();
                    Set<String> replicas = assignment.getReplicas();
                    TopicPartition topicPartition = new TopicPartition(topic, partition);
                    if (replicas.contains(commonConfiguration.getServerId())) {
                        initTopicPartitionAsync(topicPartition, assignment.getLedgerId(), assignment.getEpoch(), assignment.getConfig());
                    }
                    topicCache.get(topicPartition.topic());
                }

            }

        }).build();
        cache.listenable().addListener(listener);
        cache.start();
    }

    /**
     * Creates a new topic with the specified name, number of partitions, and replicas.
     * If the specified topic configuration is null, a default configuration is used.
     *
     * @param topic the name of the topic to be created
     * @param partitions the number of partitions for the topic
     * @param replicas the number of replicas for each partition
     * @param topicConfig the configuration for the topic; if null, a default configuration will be used
     * @return a map containing the generated topic ID and partition replicas info
     * @throws Exception if an error occurs during the creation of the topic or if there are not enough broker nodes for the specified number of replicas
     */
    @Override
    public Map<String, Object> createTopic(String topic, int partitions, int replicas, TopicConfig topicConfig) throws Exception {
        List<Node> clusterUpNodes = manager.getClusterManager().getClusterReadyNodes();
        if (clusterUpNodes.size() < replicas) {
            throw new TopicHandleException("The broker counts is not enough to assign replicas");
        }

        try {
            Collections.shuffle(clusterUpNodes);
            topicConfig = topicConfig == null
                    ? new TopicConfig(segmentConfiguration.getSegmentRollingSize(), segmentConfiguration.getSegmentRetainLimit(), segmentConfiguration.getSegmentRetainTimeMilliseconds(), false)
                    : topicConfig;

            Map<String, Object> createResult = new HashMap<>(2);
            Map<Integer, Set<String>> partitionReplicas = new HashMap<>(partitions);
            client.createContainers(PathConstants.BROKERS_TOPICS);
            List<CuratorOp> ops = new ArrayList<>(partitions + 3);
            String topicsPath = String.format(PathConstants.BROKER_TOPIC, topic);
            CuratorOp topicOp = client.transactionOp().create().withMode(CreateMode.PERSISTENT).forPath(topicsPath, null);
            ops.add(topicOp);

            String partitionsPath = String.format(PathConstants.BROKER_TOPIC_PARTITIONS, topic);
            CuratorOp partitionsOp = client.transactionOp().create().withMode(CreateMode.PERSISTENT).forPath(partitionsPath, null);
            ops.add(partitionsOp);

            for (int i = 0; i < partitions; i++) {
                Set<String> replicaNodes = assignParticipantsToNode(topic + "#" + i, replicas);
                partitionReplicas.put(i, replicaNodes);
                String partitionPath = String.format(PathConstants.BROKER_TOPIC_PARTITION, topic, i);
                TopicAssignment assignment = new TopicAssignment();
                assignment.setPartition(i);
                assignment.setTopic(topic);
                assignment.setLedgerId(generateLedgerId());
                assignment.setReplicas(replicaNodes);
                assignment.setConfig(topicConfig);
                CuratorOp partitionOp = client.transactionOp().create().withMode(CreateMode.PERSISTENT)
                        .forPath(partitionPath, SerializeFeatureSupport.serialize(assignment));
                ops.add(partitionOp);
            }

            String topicIdPath = String.format(PathConstants.BROKER_TOPIC_ID, topic);
            int topicId = generateTopicId();
            CuratorOp topicIdOp = client.transactionOp().create().withMode(CreateMode.PERSISTENT)
                    .forPath(topicIdPath, String.valueOf(topicId).getBytes(StandardCharsets.UTF_8));
            ops.add(topicIdOp);

            client.transaction().forOperations(ops);
            createResult.put(CorrelationIdConstants.TOPIC_ID, topicId);
            createResult.put(CorrelationIdConstants.PARTITION_REPLICAS, partitionReplicas);
            return createResult;
        } catch (KeeperException.NodeExistsException e) {
            throw new TopicHandleException(String.format("Topic[%s] already exists", topic));
        }
    }

    /**
     * Assigns participants to a node based on the provided token and number of participants.
     *
     * @param token the token used for node assignment
     * @param participants the number of participants to be assigned to the node
     * @return a set of node identifiers to which the participants are assigned
     */
    private Set<String> assignParticipantsToNode(String token, int participants) {
        return hashingRing.route2Nodes(token, participants);
    }

    /**
     * Generates a unique ledger ID by incrementing the current value and posting it.
     *
     * @return The generated ledger ID as an integer.
     * @throws Exception if an error occurs during the ledger ID generation process.
     */
    private int generateLedgerId() throws Exception {
        return ledgerIdGenerator.increment().postValue();
    }

    /**
     * Generates a unique topic identifier.
     * <p>
     * This method uses the topicIdGenerator to increment and fetch a new
     * topic ID to ensure uniqueness across the topics managed by the
     * system.
     *
     * @return the generated unique topic identifier
     * @throws Exception if there is an error during the ID generation process
     */
    private int generateTopicId() throws Exception {
        return topicIdGenerator.increment().postValue();
    }

    /**
     * Deletes a topic from the Zookeeper client.
     *
     * @param topic the name of the topic to be deleted
     * @throws Exception if the topic does not exist or if there is an error during the deletion process
     */
    @Override
    public void deleteTopic(String topic) throws Exception {
        DeleteBuilder deleteBuilder = client.delete();
        String path = String.format(PathConstants.BROKER_TOPIC, topic);
        try {
            deleteBuilder.guaranteed().deletingChildrenIfNeeded().forPath(path);
        } catch (Exception e) {
            throw new TopicHandleException(String.format("Topic[%s] does not exist", topic));
        }
    }

    /**
     * Initializes a topic partition asynchronously.
     *
     * @param topicPartition the topic partition to initialize
     * @param ledgerId the unique identifier for the ledger
     * @param epoch the epoch number for initialization
     * @param topicConfig the configuration for the topic
     */
    private void initTopicPartitionAsync(TopicPartition topicPartition, int ledgerId, int epoch, TopicConfig topicConfig) {
        List<EventExecutor> auxEventExecutors = manager.getAuxEventExecutors();
        EventExecutor executor = auxEventExecutors.get((Objects.hashCode(topicPartition) & 0x7fffffff) % auxEventExecutors.size());
        executor.submit(() -> {
            try {
                initPartition(topicPartition, ledgerId, epoch, topicConfig);
            } catch (Exception e) {
                throw new TopicHandleException(String.format("Async init ledger[%s] failed", ledgerId), e);
            }
        });
    }

    /**
     * Initializes a partition for a given topic.
     *
     * @param topicPartition the topic partition to initialize
     * @param ledgerId the ID of the ledger associated with the partition
     * @param epoch the epoch number for the partition
     * @param topicConfig the configuration for the topic
     * @throws Exception if an error occurs during partition initialization
     */
    @Override
    public void initPartition(TopicPartition topicPartition, int ledgerId, int epoch, TopicConfig topicConfig) throws Exception {
        LogHandler handler = manager.getLogHandler();
        if (handler.contains(ledgerId)) {
            return;
        }

        Log log = handler.initLog(topicPartition, ledgerId, epoch, topicConfig);
        ZookeeperPartitionElector
                partitionLeaderElector =
                new ZookeeperPartitionElector(commonConfiguration, zookeeperConfiguration, topicPartition, manager,
                        participantSupport, ledgerId);
        partitionLeaderElector.elect();
        leaderElectorMap.put(ledgerId, partitionLeaderElector);

        for (TopicListener topicListener : listeners) {
            topicListener.onPartitionInit(topicPartition, ledgerId);
        }

        log.start(null);
    }

    /**
     * Checks if the current node has the leadership for the specified ledger.
     *
     * @param ledger the ledger id to check for leadership
     * @return true if the current node is the leader for the specified ledger, false otherwise
     */
    @Override
    public boolean hasLeadership(int ledger) {
        ZookeeperPartitionElector partitionLeaderElector = leaderElectorMap.get(ledger);
        if (partitionLeaderElector == null) {
            return false;
        }
        return partitionLeaderElector.isLeader();
    }

    /**
     * Retires the given topic partition by updating its assignment state in ZooKeeper
     * and unsynchronizing the ledger.
     *
     * @param topicPartition The TopicPartition object specifying the topic and partition to be retired.
     * @throws Exception if there is an error during the retirement process.
     */
    @Override
    public void retirePartition(TopicPartition topicPartition) throws Exception {
        try {
            String partitionPath = String.format(PathConstants.BROKER_TOPIC_PARTITION, topicPartition.topic(), topicPartition.partition());
            Stat stat = new Stat();
            byte[] bytes = client.getData().storingStatIn(stat).forPath(partitionPath);
            TopicAssignment assignment = SerializeFeatureSupport.deserialize(bytes, TopicAssignment.class);
            int ledgerId = assignment.getLedgerId();
            assignment.setTransitionalLeader(null);
            client.setData().withVersion(stat.getVersion())
                    .forPath(partitionPath, SerializeFeatureSupport.serialize(assignment));
            Log log = manager.getLogHandler().getLog(ledgerId);
            participantSupport.unSyncLedger(topicPartition, ledgerId, log.getSyncChannel(), 30000, null);
        } catch (KeeperException.NoNodeException e) {
            throw new RuntimeException(String.format(
                    "Partition[topic=%s, partition=%d] does not exist", topicPartition.topic(), topicPartition.partition()
            ));
        } catch (Exception e) {
            retirePartition(topicPartition);
        }
    }

    /**
     * Handles the partition transfer process by updating the leader and replicas in the topic assignment.
     *
     * @param heir the new leader for the partition.
     * @param topicPartition the topic partition identifier which includes the topic name and partition number.
     * @throws Exception if any error occurs during the transfer process or if the partition does not exist.
     */
    @Override
    public void handoverPartition(String heir, TopicPartition topicPartition) throws Exception {
        try {
            String partitionPath = String.format(PathConstants.BROKER_TOPIC_PARTITION, topicPartition.topic(), topicPartition.partition());
            Stat stat = new Stat();
            byte[] bytes = client.getData().storingStatIn(stat).forPath(partitionPath);
            TopicAssignment assignment = SerializeFeatureSupport.deserialize(bytes, TopicAssignment.class);
            String leader = assignment.getLeader();
            assignment.setTransitionalLeader(leader);
            assignment.setLeader(heir);
            assignment.getReplicas().remove(leader);
            assignment.getReplicas().add(heir);
            client.setData().withVersion(stat.getVersion())
                    .forPath(partitionPath, SerializeFeatureSupport.serialize(assignment));
        } catch (KeeperException.NoNodeException e) {
            throw new TopicHandleException(String.format(
                    "Partition[topic=%s, partition=%d] does not exist", topicPartition.topic(), topicPartition.partition()
            ));
        } catch (Exception e) {
            handoverPartition(heir, topicPartition);
        }
    }

    /**
     * This method takes over leadership of a specified partition of a topic.
     * It increments the epoch of the partition assignment and reinitializes the partition
     * with the new epoch value.
     *
     * @param topicPartition The topic partition to take over, specified by a TopicPartition object
     *                       containing the topic name and partition number.
     * @throws Exception If an error occurs while trying to take over the partition.
     */
    @Override
    public void takeoverPartition(TopicPartition topicPartition) throws Exception {
        try {
            String partitionPath = String.format(PathConstants.BROKER_TOPIC_PARTITION, topicPartition.topic(), topicPartition.partition());
            Stat stat = new Stat();
            byte[] bytes = client.getData().storingStatIn(stat).forPath(partitionPath);
            TopicAssignment assignment = SerializeFeatureSupport.deserialize(bytes, TopicAssignment.class);
            assignment.setEpoch(assignment.getEpoch() + 1);
            client.setData().withVersion(stat.getVersion())
                    .forPath(partitionPath, SerializeFeatureSupport.serialize(assignment));
            initPartition(topicPartition, assignment.getLedgerId(), assignment.getEpoch(), assignment.getConfig());
        } catch (KeeperException.NoNodeException e) {
            throw new TopicHandleException(String.format(
                    "Partition[topic=%s, partition=%d] does not exist", topicPartition.topic(), topicPartition.partition()
            ));
        } catch (Exception e) {
            takeoverPartition(topicPartition);
        }
    }

    /**
     * Retrieves all topic names from ZooKeeper.
     *
     * @return a Set containing all topic names managed within ZooKeeper.
     * @throws Exception if there is an error communicating with ZooKeeper.
     */
    private Set<String> getAllTopicsFromZookeeper() throws Exception {
        List<String> topics = client.getChildren().forPath(PathConstants.BROKERS_TOPICS);
        return new HashSet<>(topics);
    }

    /**
     * Retrieves all topic names from the topic cache.
     *
     * @return a set containing all topic names
     * @throws Exception if there is an error retrieving topic names from the cache
     */
    @Override
    public Set<String> getAllTopics() throws Exception {
        return topicNamesCache.get(ALL_TOPIC_KEY);
    }

    /**
     * Asynchronously destroys a topic partition using the provided ledger ID.
     *
     * @param topicPartition The topic partition to be destroyed.
     * @param ledgerId The ID of the ledger associated with the topic partition.
     * @throws Exception If an error occurs during the destruction process.
     */
    private void destroyTopicPartitionAsync(TopicPartition topicPartition, int ledgerId) throws Exception {
        List<EventExecutor> auxEventExecutors = manager.getAuxEventExecutors();
        EventExecutor executor = auxEventExecutors.get((Objects.hashCode(topicPartition) & 0x7fffffff) % auxEventExecutors.size());
        executor.submit(() -> {
            try {
                destroyTopicPartition(topicPartition, ledgerId);
            } catch (Throwable t) {
                throw new TopicHandleException("Destroy partition failed", t);
            }
        });
    }

    /**
     * Destroys the given topic partition by shutting down its leader elector,
     * destroying its log, and notifying all listeners of its destruction.
     *
     * @param topicPartition the topic partition to be destroyed
     * @param ledgerId the ledger ID associated with the partition
     * @throws Exception if an error occurs during the partition destruction process
     */
    @Override
    public void destroyTopicPartition(TopicPartition topicPartition, int ledgerId) throws Exception {
        ZookeeperPartitionElector partitionLeaderElector = leaderElectorMap.remove(ledgerId);
        partitionLeaderElector.shutdown();
        manager.getLogHandler().destroyLog(ledgerId);
        for (TopicListener topicListener : listeners) {
            topicListener.onPartitionDestroy(topicPartition, ledgerId);
        }
    }

    /**
     * Retrieves the partition information for a given topic partition.
     *
     * @param topicPartition the topic partition for which information is requested
     * @return the partition information if available, otherwise null
     * @throws Exception if an error occurs while retrieving the information
     */
    @Override
    public PartitionInfo getPartitionInfo(TopicPartition topicPartition) throws Exception {
        Set<PartitionInfo> partitionInfos = topicCache.get(topicPartition.topic());
        if (partitionInfos == null || partitionInfos.isEmpty()) {
            return null;
        }

        for (PartitionInfo info : partitionInfos) {
            int partition = info.getPartition();
            if (partition == topicPartition.partition()) {
                return info;
            }
        }
        return null;
    }

    /**
     * Shuts down the ZookeeperTopicHandleSupport instance and its associated resources.
     * <p>
     * This method performs the following operations:
     * 1. If the cache is not null, it closes the cache to release any held resources.
     * 2. Calls the shutdown method on the participantSupport to ensure any associated tasks are completed or terminated.
     * 3. Clears the leaderElectorMap to remove any stored leadership information.
     *
     * @throws Exception if an error occurs during the shutdown process.
     */
    @Override
    public void shutdown() throws Exception {
        if (cache != null) {
            cache.close();
        }
        participantSupport.shutdown();
        leaderElectorMap.clear();
    }

    /**
     * Retrieves partition information for a given topic from the cache.
     *
     * @param topic the name of the topic for which partition information is requested
     * @return a set of {@link PartitionInfo} objects containing information about the partitions of the specified topic
     */
    @Override
    public Set<PartitionInfo> getTopicInfo(String topic) {
        return topicCache.get(topic);
    }

    /**
     * Retrieves a list of topic listeners.
     *
     * @return a List of TopicListener objects.
     */
    @Nonnull
    @Override
    public List<TopicListener> getTopicListener() {
        return listeners;
    }

    /**
     * Adds a listener for topic-related events. The provided listener will be notified of events such as
     * partition initialization, partition destruction, leader assignment, leader loss, topic creation,
     * topic deletion, and partition assignment changes.
     *
     * @param listener the listener to be added for receiving topic-related events
     */
    @Override
    public void addTopicListener(TopicListener listener) {
        listeners.add(listener);
    }

    /**
     * Retrieves the ParticipantSupport instance.
     *
     * @return the instance of ParticipantSupport
     */
    @Override
    public ParticipantSupport getParticipantSuooprt() {
        return participantSupport;
    }

    /**
     * Calculates the number of partitions for each topic present in the ZooKeeper.
     * This method fetches the list of topics and the corresponding nodes for each topic,
     * then computes the number of partitions by subtracting one from the size of nodes list.
     *
     * @return A map where the keys are topic names and the values represent the number of partitions for those topics.
     * @throws Exception if there is any issue accessing data from ZooKeeper.
     */
    @Override
    public Map<String, Integer> calculatePartitions() throws Exception {
        List<String> topics = client.getChildren().forPath(PathConstants.BROKERS_TOPICS);
        Map<String, Integer> result = new HashMap<>();
        for (String topic : topics) {
            List<String> nodes = client.getChildren().forPath(String.format(PathConstants.BROKER_TOPIC, topic));
            result.put(topic, nodes.size() - 1);
        }
        return result;
    }
}
