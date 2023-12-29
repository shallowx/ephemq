package org.meteor.coordinatior;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.netty.util.concurrent.EventExecutor;
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
import org.meteor.common.message.TopicConfig;
import org.meteor.common.message.TopicPartition;
import org.meteor.config.*;
import org.meteor.internal.CorrelationIdConstants;
import org.meteor.internal.PathConstants;
import org.meteor.internal.ZookeeperClient;
import org.meteor.ledger.Log;
import org.meteor.ledger.LogCoordinator;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.listener.TopicListener;

import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ZookeeperTopicCoordinator implements TopicCoordinator {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ZookeeperClusterCoordinator.class);
    protected static final String ALL_TOPIC_KEY = "ALL-TOPIC";
    private static final String TOPIC_PARTITION_REGEX = "^/brokers/topics/[\\w\\-#]+/partitions/\\d+$";
    private static final String TOPIC_REGEX = "^/brokers/topics/[\\w\\-#]+$";
    protected final Map<Integer, ZookeeperPartitionCoordinatorElector> leaderElectorMap = new ConcurrentHashMap<>();
    protected final List<TopicListener> listeners = new LinkedList<>();
    protected CommonConfig commonConfiguration;
    protected SegmentConfig segmentConfiguration;
    private ZookeeperConfig zookeeperConfiguration;
    protected CuratorFramework client;
    protected CuratorCache cache;
    protected ParticipantCoordinator participantCoordinator;
    protected DistributedAtomicInteger topicIdGenerator;
    protected DistributedAtomicInteger ledgerIdGenerator;
    protected Coordinator coordinator;
    protected LoadingCache<String, Set<org.meteor.common.message.PartitionInfo>> topicCache;
    protected LoadingCache<String, Set<String>> topicNamesCache;

    public ZookeeperTopicCoordinator() {
    }

    public ZookeeperTopicCoordinator(ServerConfig config, Coordinator coordinator) {
        this.commonConfiguration = config.getCommonConfig();
        this.segmentConfiguration = config.getSegmentConfig();
        this.zookeeperConfiguration = config.getZookeeperConfig();
        this.coordinator = coordinator;
        this.participantCoordinator = new ParticipantCoordinator(coordinator);
        this.client = ZookeeperClient.getClient(config.getZookeeperConfig(), commonConfiguration.getClusterName());
        this.topicIdGenerator = new DistributedAtomicInteger(this.client, CorrelationIdConstants.TOPIC_ID_COUNTER, new RetryOneTime(100));
        this.ledgerIdGenerator = new DistributedAtomicInteger(this.client, CorrelationIdConstants.LEDGER_ID_COUNTER, new RetryOneTime(100));
        this.topicCache = Caffeine.newBuilder().refreshAfterWrite(1, TimeUnit.MINUTES)
                .build(new CacheLoader<>() {
                    @Override
                    public @Nullable Set<org.meteor.common.message.PartitionInfo> load(String topic) throws Exception {
                        try {
                            return getTopicInfoFromZookeeper(topic);
                        } catch (IllegalArgumentException e) {
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

    private void refreshPartitionInfo(String topic, Stat stat, org.meteor.common.message.TopicAssignment assignment) {
        String path = String.format(PathConstants.BROKER_TOPIC_ID, topic);
        try {
            byte[] bytes = client.getData().forPath(path);
            int topicId = Integer.parseInt(new String(bytes, StandardCharsets.UTF_8));
            int partition = assignment.getPartition();
            String leader = assignment.getLeader();
            Set<String> replicas = assignment.getReplicas();
            int ledgerId = assignment.getLedgerId();
            int epoch = assignment.getEpoch();
            org.meteor.common.message.TopicConfig topicConfig = assignment.getConfig();
            org.meteor.common.message.PartitionInfo partitionInfo = new org.meteor.common.message.PartitionInfo(topic, topicId, partition, ledgerId, epoch, leader, replicas, topicConfig, stat.getVersion());
            Set<org.meteor.common.message.PartitionInfo> partitionInfos = topicCache.get(topic);
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

    private Set<org.meteor.common.message.PartitionInfo> getTopicInfoFromZookeeper(String topic) throws Exception {
        String partitionsPath = String.format(PathConstants.BROKER_TOPIC_PARTITIONS, topic);
        String topicIdPath = String.format(PathConstants.BROKER_TOPIC_ID, topic);
        try {
            byte[] bytes = client.getData().forPath(topicIdPath);
            int topicId = Integer.parseInt(new String(bytes, StandardCharsets.UTF_8));
            List<String> partitions = client.getChildren().forPath(partitionsPath);
            if (partitions == null || partitions.isEmpty()) {
                return null;
            }

            Set<org.meteor.common.message.PartitionInfo> partitionInfos = new ObjectOpenHashSet<>();
            for (String nodeName : partitions) {
                int partition = Integer.parseInt(nodeName);
                String partitionPath = String.format(PathConstants.BROKER_TOPIC_PARTITION, topic, partition);
                Stat stat = new Stat();
                bytes = client.getData().storingStatIn(stat).forPath(partitionPath);
                org.meteor.common.message.TopicAssignment assignment = JsonMapper.deserialize(bytes, org.meteor.common.message.TopicAssignment.class);
                String leader = assignment.getLeader();
                Set<String> replicas = assignment.getReplicas();
                int ledgerId = assignment.getLedgerId();
                int epoch = assignment.getEpoch();
                org.meteor.common.message.TopicConfig topicConfig = assignment.getConfig();
                org.meteor.common.message.PartitionInfo partitionInfo = new org.meteor.common.message.PartitionInfo(topic, topicId, partition, ledgerId, epoch, leader, replicas, topicConfig, stat.getVersion());
                partitionInfos.add(partitionInfo);
            }
            return partitionInfos;
        } catch (Exception e) {
            throw new IllegalStateException(String.format("Topic %s dose not exist", topic));
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
                    org.meteor.common.message.TopicAssignment assignment = JsonMapper.deserialize(nodeData, org.meteor.common.message.TopicAssignment.class);
                    assignment.setVersion(version);

                    Set<String> replicas = assignment.getReplicas();
                    byte[] oldNodeData = oldData.getData();
                    int oldVersion = oldData.getStat().getVersion();
                    org.meteor.common.message.TopicAssignment oldAssignment = JsonMapper.deserialize(oldNodeData, org.meteor.common.message.TopicAssignment.class);
                    oldAssignment.setVersion(oldVersion);

                    org.meteor.common.message.TopicPartition topicPartition = new org.meteor.common.message.TopicPartition(assignment.getTopic(), assignment.getPartition());
                    refreshPartitionInfo(topicPartition.getTopic(), data.getStat(), assignment);
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
                    org.meteor.common.message.TopicAssignment assignment = JsonMapper.deserialize(nodeData, org.meteor.common.message.TopicAssignment.class);
                    String topic = assignment.getTopic();
                    int partition = assignment.getPartition();
                    Set<String> replicas = assignment.getReplicas();
                    org.meteor.common.message.TopicPartition topicPartition = new org.meteor.common.message.TopicPartition(topic, partition);
                    if (replicas.contains(commonConfiguration.getServerId())) {
                        destroyTopicPartitionAsync(topicPartition, assignment.getLedgerId());
                    }
                    topicCache.invalidate(topicPartition.getTopic());
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
                    org.meteor.common.message.TopicAssignment assignment = JsonMapper.deserialize(nodeData, org.meteor.common.message.TopicAssignment.class);
                    String topic = assignment.getTopic();
                    int partition = assignment.getPartition();
                    Set<String> replicas = assignment.getReplicas();
                    org.meteor.common.message.TopicPartition topicPartition = new org.meteor.common.message.TopicPartition(topic, partition);
                    if (replicas.contains(commonConfiguration.getServerId())) {
                        initTopicPartitionAsync(topicPartition, assignment.getLedgerId(), assignment.getEpoch(), assignment.getConfig());
                    }
                    topicCache.get(topicPartition.getTopic());
                }

            }

        }).build();
        cache.listenable().addListener(listener);
        cache.start();
    }

    @Override
    public Map<String, Object> createTopic(String topic, int partitions, int replicas, org.meteor.common.message.TopicConfig topicConfig) throws Exception {
        List<org.meteor.common.message.Node> clusterUpNodes = coordinator.getClusterCoordinator().getClusterUpNodes();
        if (clusterUpNodes.size() < replicas) {
            throw new IllegalStateException("The broker counts is not enough to assign replicas");
        }
        try {
            Collections.shuffle(clusterUpNodes);
            topicConfig = topicConfig == null
                    ? new org.meteor.common.message.TopicConfig(segmentConfiguration.getSegmentRollingSize(), segmentConfiguration.getSegmentRetainLimit(), segmentConfiguration.getSegmentRetainTime(), false)
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
                Set<String> partitionReplicaSet = new TreeSet<>();
                for (int j = 0; j < replicas; j++) {
                    int nodeIdx = (i + j) % clusterUpNodes.size();
                    org.meteor.common.message.Node node = clusterUpNodes.get(nodeIdx);
                    partitionReplicaSet.add(node.getId());
                }

                partitionReplicas.put(i, partitionReplicaSet);
                String partitionPath = String.format(PathConstants.BROKER_TOPIC_PARTITION, topic, i);
                org.meteor.common.message.TopicAssignment assignment = new org.meteor.common.message.TopicAssignment();
                assignment.setPartition(i);
                assignment.setTopic(topic);
                assignment.setEpoch(-1);
                assignment.setLedgerId(generateLedgerId());
                assignment.setReplicas(partitionReplicaSet);
                assignment.setConfig(topicConfig);
                CuratorOp partitionOp = client.transactionOp().create().withMode(CreateMode.PERSISTENT)
                        .forPath(partitionPath, JsonMapper.serialize(assignment));
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
            throw new IllegalStateException(String.format("Topic %s already exists", topic));
        }
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
            throw new IllegalStateException(String.format("Topic %s does not exist", topic));
        }
    }

    private void initTopicPartitionAsync(TopicPartition topicPartition, int ledgerId, int epoch, TopicConfig topicConfig) {
        List<EventExecutor> auxEventExecutors = coordinator.getAuxEventExecutors();
        EventExecutor executor = auxEventExecutors.get((Objects.hashCode(topicPartition) & 0x7fffffff) % auxEventExecutors.size());
        executor.submit(() -> {
            try {
                initPartition(topicPartition, ledgerId, epoch, topicConfig);
            } catch (Exception e) {
                throw new RuntimeException("Init log failed", e);
            }
        });
    }

    @Override
    public void initPartition(org.meteor.common.message.TopicPartition topicPartition, int ledgerId, int epoch, org.meteor.common.message.TopicConfig topicConfig) throws Exception {
        LogCoordinator logCoordinator = coordinator.getLogCoordinator();
        if (logCoordinator.contains(ledgerId)) {
            return;
        }

        Log log = logCoordinator.initLog(topicPartition, ledgerId, epoch, topicConfig);
        ZookeeperPartitionCoordinatorElector partitionLeaderElector = new ZookeeperPartitionCoordinatorElector(commonConfiguration, zookeeperConfiguration, topicPartition, coordinator, participantCoordinator, ledgerId);
        partitionLeaderElector.elect();
        leaderElectorMap.put(ledgerId, partitionLeaderElector);

        for (TopicListener topicListener : listeners) {
            topicListener.onPartitionInit(topicPartition, ledgerId);
        }

        log.start(null);
    }

    @Override
    public boolean hasLeadership(int ledger) {
        ZookeeperPartitionCoordinatorElector partitionLeaderElector = leaderElectorMap.get(ledger);
        if (partitionLeaderElector == null) {
            return false;
        }
        return partitionLeaderElector.isLeader();
    }

    @Override
    public void retirePartition(org.meteor.common.message.TopicPartition topicPartition) throws Exception {
        try {
            String partitionPath = String.format(PathConstants.BROKER_TOPIC_PARTITION, topicPartition.getTopic(), topicPartition.getPartition());
            Stat stat = new Stat();
            byte[] bytes = client.getData().storingStatIn(stat).forPath(partitionPath);
            org.meteor.common.message.TopicAssignment assignment = JsonMapper.deserialize(bytes, org.meteor.common.message.TopicAssignment.class);
            int ledgerId = assignment.getLedgerId();
            assignment.setTransitionalLeader(null);
            client.setData().withVersion(stat.getVersion()).forPath(partitionPath, JsonMapper.serialize(assignment));
            Log log = coordinator.getLogCoordinator().getLog(ledgerId);
            participantCoordinator.unSyncLedger(topicPartition, ledgerId, log.getSyncChannel(), 30000, null);
        } catch (KeeperException.NoNodeException e) {
            throw new RuntimeException(String.format(
                    "Partition[topic=%s partition=%d] does not exist", topicPartition.getTopic(), topicPartition.getPartition()
            ));
        } catch (Exception e) {
            retirePartition(topicPartition);
        }
    }

    @Override
    public void handoverPartition(String heir, org.meteor.common.message.TopicPartition topicPartition) throws Exception {
        try {
            String partitionPath = String.format(PathConstants.BROKER_TOPIC_PARTITION, topicPartition.getTopic(), topicPartition.getPartition());
            Stat stat = new Stat();
            byte[] bytes = client.getData().storingStatIn(stat).forPath(partitionPath);
            org.meteor.common.message.TopicAssignment assignment = JsonMapper.deserialize(bytes, org.meteor.common.message.TopicAssignment.class);
            String leader = assignment.getLeader();
            assignment.setTransitionalLeader(leader);
            assignment.setLeader(heir);
            assignment.getReplicas().remove(leader);
            assignment.getReplicas().add(heir);
            client.setData().withVersion(stat.getVersion()).forPath(partitionPath, JsonMapper.serialize(assignment));
        } catch (KeeperException.NoNodeException e) {
            throw new RuntimeException(String.format(
                    "Partition[topic=%s partition=%d] does not exist", topicPartition.getTopic(), topicPartition.getPartition()
            ));
        } catch (Exception e) {
            handoverPartition(heir, topicPartition);
        }
    }

    @Override
    public void takeoverPartition(org.meteor.common.message.TopicPartition topicPartition) throws Exception {
        try {
            String partitionPath = String.format(PathConstants.BROKER_TOPIC_PARTITION, topicPartition.getTopic(), topicPartition.getPartition());
            Stat stat = new Stat();
            byte[] bytes = client.getData().storingStatIn(stat).forPath(partitionPath);
            org.meteor.common.message.TopicAssignment assignment = JsonMapper.deserialize(bytes, org.meteor.common.message.TopicAssignment.class);
            assignment.setEpoch(assignment.getEpoch() + 1);
            client.setData().withVersion(stat.getVersion()).forPath(partitionPath, JsonMapper.serialize(assignment));
            initPartition(topicPartition, assignment.getLedgerId(), assignment.getEpoch(), assignment.getConfig());
        } catch (KeeperException.NoNodeException e) {
            throw new RuntimeException(String.format(
                    "Partition[topic=%s partition=%d] does not exist", topicPartition.getTopic(), topicPartition.getPartition()
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
                throw new RuntimeException("Destroy partition failed", t);
            }
        });
    }

    @Override
    public void destroyTopicPartition(org.meteor.common.message.TopicPartition topicPartition, int ledgerId) throws Exception {
        ZookeeperPartitionCoordinatorElector partitionLeaderElector = leaderElectorMap.remove(ledgerId);
        partitionLeaderElector.shutdown();
        coordinator.getLogCoordinator().destroyLog(ledgerId);
        for (TopicListener topicListener : listeners) {
            topicListener.onPartitionDestroy(topicPartition, ledgerId);
        }
    }

    @Override
    public org.meteor.common.message.PartitionInfo getPartitionInfo(TopicPartition topicPartition) throws Exception {
        Set<org.meteor.common.message.PartitionInfo> partitionInfos = topicCache.get(topicPartition.getTopic());
        if (partitionInfos == null || partitionInfos.isEmpty()) {
            return null;
        }

        for (org.meteor.common.message.PartitionInfo info : partitionInfos) {
            int partition = info.getPartition();
            if (partition == topicPartition.getPartition()) {
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
    public Set<org.meteor.common.message.PartitionInfo> getTopicInfo(String topic) {
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
