package org.meteor.support;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.netty.util.concurrent.EventExecutor;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.DeleteBuilder;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicInteger;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.RetryOneTime;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.common.message.Node;
import org.meteor.common.message.PartitionInfo;
import org.meteor.common.message.TopicAssignment;
import org.meteor.common.message.TopicConfig;
import org.meteor.common.message.TopicPartition;
import org.meteor.config.CommonConfig;
import org.meteor.config.SegmentConfig;
import org.meteor.config.ServerConfig;
import org.meteor.config.ZookeeperConfig;
import org.meteor.exception.TopicException;
import org.meteor.internal.CorrelationIdConstants;
import org.meteor.internal.ZookeeperClientFactory;
import org.meteor.ledger.Log;
import org.meteor.ledger.LogHandler;
import org.meteor.listener.TopicListener;

public class ZookeeperTopicCoordinator implements TopicCoordinator {
    protected static final String ALL_TOPIC_KEY = "ALL-TOPIC";
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ZookeeperClusterManager.class);
    private static final String TOPIC_PARTITION_REGEX = "^/brokers/topics/[\\w\\-#]+/partitions/\\d+$";
    private static final String TOPIC_REGEX = "^/brokers/topics/[\\w\\-#]+$";
    protected final Map<Integer, ZookeeperPartitionElector> leaderElectorMap = new ConcurrentHashMap<>();
    protected final List<TopicListener> listeners = new LinkedList<>();
    private final ConsistentHashingRing hashingRing;
    protected CommonConfig commonConfiguration;
    protected SegmentConfig segmentConfiguration;
    protected CuratorFramework client;
    protected CuratorCache cache;
    protected ParticipantCoordinator participantCoordinator;
    protected DistributedAtomicInteger topicIdGenerator;
    protected DistributedAtomicInteger ledgerIdGenerator;
    protected Manager coordinator;
    protected LoadingCache<String, Set<PartitionInfo>> topicCache;
    protected LoadingCache<String, Set<String>> topicNamesCache;
    private ZookeeperConfig zookeeperConfiguration;

    public ZookeeperTopicCoordinator() {
        this.hashingRing = null;
    }

    public ZookeeperTopicCoordinator(ServerConfig config, Manager coordinator, ConsistentHashingRing hashingRing) {
        this.commonConfiguration = config.getCommonConfig();
        this.segmentConfiguration = config.getSegmentConfig();
        this.zookeeperConfiguration = config.getZookeeperConfig();
        this.coordinator = coordinator;
        this.hashingRing = hashingRing;
        this.participantCoordinator = new ParticipantCoordinator(coordinator);
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
                TopicAssignment assignment = JsonFeatureMapper.deserialize(bytes, TopicAssignment.class);
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
            throw new TopicException(String.format("Topic[%s] dose not exist", topic));
        }
    }

    @Override
    public void start() throws Exception {
        participantCoordinator.start();
        cache = CuratorCache.build(client, PathConstants.BROKERS_TOPICS);
        CuratorCacheListener listener = CuratorCacheListener.builder().forTreeCache(client, new TreeCacheListener() {
            final Pattern TOPIC_PARTITION = Pattern.compile(TOPIC_PARTITION_REGEX);
            final Pattern TOPIC = Pattern.compile(TOPIC_REGEX);

            @Override
            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent event) throws Exception {
                TreeCacheEvent.Type type = event.getType();
                switch (type) {
                    case NODE_ADDED -> handleAdd(event);
                    case NODE_REMOVED -> handleRemove(event);
                    case NODE_UPDATED -> handleUpdated(event);
                }
            }

            boolean regularExpressionMatcher(String origin, Pattern pattern) {
                if (pattern == null) {
                    return true;
                }
                Matcher matcher = pattern.matcher(origin);
                return matcher.matches();
            }

            private boolean isTopicPartitionNode(String path) {
                return regularExpressionMatcher(path, TOPIC_PARTITION);
            }

            private boolean isTopicNode(String path) {
                return regularExpressionMatcher(path, TOPIC);
            }

            private void handleUpdated(TreeCacheEvent event) throws Exception {
                ChildData data = event.getData();
                ChildData oldData = event.getOldData();
                String path = data.getPath();
                if (isTopicPartitionNode(path)) {
                    byte[] nodeData = data.getData();
                    int version = data.getStat().getVersion();
                    TopicAssignment assignment = JsonFeatureMapper.deserialize(nodeData, TopicAssignment.class);
                    assignment.setVersion(version);

                    Set<String> replicas = assignment.getReplicas();
                    byte[] oldNodeData = oldData.getData();
                    int oldVersion = oldData.getStat().getVersion();
                    TopicAssignment oldAssignment = JsonFeatureMapper.deserialize(oldNodeData, TopicAssignment.class);
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
                    TopicAssignment assignment = JsonFeatureMapper.deserialize(nodeData, TopicAssignment.class);
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
                    TopicAssignment assignment = JsonFeatureMapper.deserialize(nodeData, TopicAssignment.class);
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

    @Override
    public Map<String, Object> createTopic(String topic, int partitions, int replicas, TopicConfig topicConfig) throws Exception {
        List<Node> clusterUpNodes = coordinator.getClusterManager().getClusterReadyNodes();
        if (clusterUpNodes.size() < replicas) {
            throw new TopicException("The broker counts is not enough to assign replicas");
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
                        .forPath(partitionPath, JsonFeatureMapper.serialize(assignment));
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
            throw new TopicException(String.format("Topic[%s] already exists", topic));
        }
    }

    private Set<String> assignParticipantsToNode(String token, int participants) {
        return hashingRing.route2Nodes(token, participants);
    }

    private int generateLedgerId() throws Exception {
        return ledgerIdGenerator.increment().postValue();
    }

    private int generateTopicId() throws Exception {
        return topicIdGenerator.increment().postValue();
    }

    @Override
    public void deleteTopic(String topic) throws Exception {
        DeleteBuilder deleteBuilder = client.delete();
        String path = String.format(PathConstants.BROKER_TOPIC, topic);
        try {
            deleteBuilder.guaranteed().deletingChildrenIfNeeded().forPath(path);
        } catch (Exception e) {
            throw new TopicException(String.format("Topic[%s] does not exist", topic));
        }
    }

    private void initTopicPartitionAsync(TopicPartition topicPartition, int ledgerId, int epoch, TopicConfig topicConfig) {
        List<EventExecutor> auxEventExecutors = coordinator.getAuxEventExecutors();
        EventExecutor executor = auxEventExecutors.get((Objects.hashCode(topicPartition) & 0x7fffffff) % auxEventExecutors.size());
        executor.submit(() -> {
            try {
                initPartition(topicPartition, ledgerId, epoch, topicConfig);
            } catch (Exception e) {
                throw new TopicException(String.format("Async init ledger[%s] failed", ledgerId), e);
            }
        });
    }

    @Override
    public void initPartition(TopicPartition topicPartition, int ledgerId, int epoch, TopicConfig topicConfig) throws Exception {
        LogHandler logCoordinator = coordinator.getLogHandler();
        if (logCoordinator.contains(ledgerId)) {
            return;
        }

        Log log = logCoordinator.initLog(topicPartition, ledgerId, epoch, topicConfig);
        ZookeeperPartitionElector
                partitionLeaderElector =
                new ZookeeperPartitionElector(commonConfiguration, zookeeperConfiguration, topicPartition, coordinator,
                        participantCoordinator, ledgerId);
        partitionLeaderElector.elect();
        leaderElectorMap.put(ledgerId, partitionLeaderElector);

        for (TopicListener topicListener : listeners) {
            topicListener.onPartitionInit(topicPartition, ledgerId);
        }

        log.start(null);
    }

    @Override
    public boolean hasLeadership(int ledger) {
        ZookeeperPartitionElector partitionLeaderElector = leaderElectorMap.get(ledger);
        if (partitionLeaderElector == null) {
            return false;
        }
        return partitionLeaderElector.isLeader();
    }

    @Override
    public void retirePartition(TopicPartition topicPartition) throws Exception {
        try {
            String partitionPath = String.format(PathConstants.BROKER_TOPIC_PARTITION, topicPartition.topic(), topicPartition.partition());
            Stat stat = new Stat();
            byte[] bytes = client.getData().storingStatIn(stat).forPath(partitionPath);
            TopicAssignment assignment = JsonFeatureMapper.deserialize(bytes, TopicAssignment.class);
            int ledgerId = assignment.getLedgerId();
            assignment.setTransitionalLeader(null);
            client.setData().withVersion(stat.getVersion()).forPath(partitionPath, JsonFeatureMapper.serialize(assignment));
            Log log = coordinator.getLogHandler().getLog(ledgerId);
            participantCoordinator.unSyncLedger(topicPartition, ledgerId, log.getSyncChannel(), 30000, null);
        } catch (KeeperException.NoNodeException e) {
            throw new RuntimeException(String.format(
                    "Partition[topic=%s, partition=%d] does not exist", topicPartition.topic(), topicPartition.partition()
            ));
        } catch (Exception e) {
            retirePartition(topicPartition);
        }
    }

    @Override
    public void handoverPartition(String heir, TopicPartition topicPartition) throws Exception {
        try {
            String partitionPath = String.format(PathConstants.BROKER_TOPIC_PARTITION, topicPartition.topic(), topicPartition.partition());
            Stat stat = new Stat();
            byte[] bytes = client.getData().storingStatIn(stat).forPath(partitionPath);
            TopicAssignment assignment = JsonFeatureMapper.deserialize(bytes, TopicAssignment.class);
            String leader = assignment.getLeader();
            assignment.setTransitionalLeader(leader);
            assignment.setLeader(heir);
            assignment.getReplicas().remove(leader);
            assignment.getReplicas().add(heir);
            client.setData().withVersion(stat.getVersion()).forPath(partitionPath, JsonFeatureMapper.serialize(assignment));
        } catch (KeeperException.NoNodeException e) {
            throw new TopicException(String.format(
                    "Partition[topic=%s, partition=%d] does not exist", topicPartition.topic(), topicPartition.partition()
            ));
        } catch (Exception e) {
            handoverPartition(heir, topicPartition);
        }
    }

    @Override
    public void takeoverPartition(TopicPartition topicPartition) throws Exception {
        try {
            String partitionPath = String.format(PathConstants.BROKER_TOPIC_PARTITION, topicPartition.topic(), topicPartition.partition());
            Stat stat = new Stat();
            byte[] bytes = client.getData().storingStatIn(stat).forPath(partitionPath);
            TopicAssignment assignment = JsonFeatureMapper.deserialize(bytes, TopicAssignment.class);
            assignment.setEpoch(assignment.getEpoch() + 1);
            client.setData().withVersion(stat.getVersion()).forPath(partitionPath, JsonFeatureMapper.serialize(assignment));
            initPartition(topicPartition, assignment.getLedgerId(), assignment.getEpoch(), assignment.getConfig());
        } catch (KeeperException.NoNodeException e) {
            throw new TopicException(String.format(
                    "Partition[topic=%s, partition=%d] does not exist", topicPartition.topic(), topicPartition.partition()
            ));
        } catch (Exception e) {
            takeoverPartition(topicPartition);
        }
    }

    private Set<String> getAllTopicsFromZookeeper() throws Exception {
        List<String> topics = client.getChildren().forPath(PathConstants.BROKERS_TOPICS);
        return new HashSet<>(topics);
    }

    @Override
    public Set<String> getAllTopics() throws Exception {
        return topicNamesCache.get(ALL_TOPIC_KEY);
    }

    private void destroyTopicPartitionAsync(TopicPartition topicPartition, int ledgerId) throws Exception {
        List<EventExecutor> auxEventExecutors = coordinator.getAuxEventExecutors();
        EventExecutor executor = auxEventExecutors.get((Objects.hashCode(topicPartition) & 0x7fffffff) % auxEventExecutors.size());
        executor.submit(() -> {
            try {
                destroyTopicPartition(topicPartition, ledgerId);
            } catch (Throwable t) {
                throw new TopicException("Destroy partition failed", t);
            }
        });
    }

    @Override
    public void destroyTopicPartition(TopicPartition topicPartition, int ledgerId) throws Exception {
        ZookeeperPartitionElector partitionLeaderElector = leaderElectorMap.remove(ledgerId);
        partitionLeaderElector.shutdown();
        coordinator.getLogHandler().destroyLog(ledgerId);
        for (TopicListener topicListener : listeners) {
            topicListener.onPartitionDestroy(topicPartition, ledgerId);
        }
    }

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

    @Override
    public void shutdown() throws Exception {
        if (cache != null) {
            cache.close();
        }
        participantCoordinator.shutdown();
        leaderElectorMap.clear();
    }

    @Override
    public Set<PartitionInfo> getTopicInfo(String topic) {
        return topicCache.get(topic);
    }

    @Override
    public List<TopicListener> getTopicListener() {
        return listeners;
    }

    @Override
    public void addTopicListener(TopicListener listener) {
        listeners.add(listener);
    }

    @Override
    public ParticipantCoordinator getParticipantCoordinator() {
        return participantCoordinator;
    }

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
