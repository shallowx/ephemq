package org.meteor.coordinator;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CreateBuilder;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.common.message.Node;
import org.meteor.config.CommonConfig;
import org.meteor.config.ServerConfig;
import org.meteor.internal.ZookeeperClientFactory;
import org.meteor.listener.ClusterListener;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ZookeeperClusterCoordinator implements ClusterCoordinator {
    private static final String UP = "UP";
    private static final String DOWN = "DOWN";
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ZookeeperClusterCoordinator.class);
    protected final List<ClusterListener> listeners = new ObjectArrayList<>();
    private final CommonConfig configuration;
    private final Map<String, Node> readyNodes = new ConcurrentHashMap<>();
    protected CuratorFramework client;
    protected ConnectionStateListener connectionStateListener;
    protected CuratorCache cache;
    private volatile boolean registered = false;
    private LeaderLatch latch;
    private Node thisNode;

    public ZookeeperClusterCoordinator(ServerConfig config) {
        this.configuration = config.getCommonConfig();
        this.client = ZookeeperClientFactory.getReadyClient(config.getZookeeperConfig(), config.getCommonConfig().getClusterName());
    }

    @Override
    public void start() throws Exception {
        connectionStateListener = (client, state) -> {
            if (state == ConnectionState.RECONNECTED) {
                try {
                    Stat stat = client.checkExists().forPath(String.format(PathConstants.BROKERS_ID, configuration.getServerId()));
                    if (stat != null) {
                        return;
                    }

                    registerNode(PathConstants.BROKERS_ID);
                } catch (Exception e) {
                    if (logger.isErrorEnabled()) {
                        logger.error("Re-register node failed", e);
                    }
                }
            }
        };

        client.getConnectionStateListenable().addListener(connectionStateListener);
        startBrokersListener(PathConstants.BROKERS_IDS);
        electController();
        registerNode(PathConstants.BROKERS_ID);
    }

    private void electController() throws Exception {
        latch = new LeaderLatch(client, PathConstants.CONTROLLER, configuration.getServerId(), LeaderLatch.CloseMode.NOTIFY_LEADER);
        latch.addListener(new LeaderLatchListener() {
            @Override
            public void isLeader() {
                for (ClusterListener listener : listeners) {
                    listener.onGetControlRole(thisNode);
                }
                if (logger.isInfoEnabled()) {
                    logger.info("Get the controller role");
                }
            }

            @Override
            public void notLeader() {
                for (ClusterListener listener : listeners) {
                    listener.onLostControlRole(thisNode);
                }
                if (logger.isInfoEnabled()) {
                    logger.info("Lost the controller role");
                }
            }
        });
        latch.start();
    }

    protected void startBrokersListener(String path) {
        cache = CuratorCache.build(client, path);
        CuratorCacheListener listener = CuratorCacheListener.builder()
                .forPathChildrenCache(path, client, new PathChildrenCacheListener() {
                    @Override
                    public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent event) throws Exception {
                        PathChildrenCacheEvent.Type type = event.getType();
                        switch (type) {
                            case CHILD_ADDED -> handleAdd(event);
                            case CHILD_REMOVED -> handleRemove(event);
                            case CHILD_UPDATED -> handlerUpdated(event);
                            default -> {
                                if (logger.isDebugEnabled()) {
                                    logger.debug("Unsupported child operation type[{}]", type);
                                }
                            }
                        }
                    }

                    private void handleAdd(PathChildrenCacheEvent event) throws Exception {
                        ChildData data = event.getData();
                        Node node = JsonFeatureMapper.deserialize(data.getData(), Node.class);

                        readyNodes.put(node.getId(), node);
                        for (ClusterListener listener : listeners) {
                            listener.onNodeJoin(node);
                        }
                    }

                    private void handleRemove(PathChildrenCacheEvent event) throws Exception {
                        ChildData data = event.getData();
                        Node node = JsonFeatureMapper.deserialize(data.getData(), Node.class);

                        readyNodes.remove(node.getId());
                        for (ClusterListener listener : listeners) {
                            listener.onNodeLeave(node);
                        }
                    }

                    private void handlerUpdated(PathChildrenCacheEvent event) throws Exception {
                        ChildData data = event.getData();
                        Node node = JsonFeatureMapper.deserialize(data.getData(), Node.class);

                        readyNodes.put(node.getId(), node);
                        if (DOWN.equals(node.getState())) {
                            for (ClusterListener listener : listeners) {
                                listener.onNodeDown(node);
                            }
                        }
                    }
                })
                .build();

        cache.listenable().addListener(listener);
        cache.start();
    }

    protected void registerNode(String path) throws Exception {
        CreateBuilder createBuilder = client.create();
        thisNode = new Node(configuration.getServerId(), configuration.getAdvertisedAddress(), configuration.getAdvertisedPort(),
                System.currentTimeMillis(), configuration.getClusterName(), UP);

        try {
            createBuilder.creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
                    .forPath(String.format(path, configuration.getServerId()), JsonFeatureMapper.serialize(thisNode));
            registered = true;
        } catch (KeeperException.NodeExistsException e) {
            throw new RuntimeException(String.format("Server id[%s] should be unique", configuration.getServerId()));
        }
    }

    protected void unregistered(String path) throws Exception {
        if (client == null || !registered) {
            return;
        }

        String nodePath = String.format(path, configuration.getServerId());
        updateNodeStateAndSleep(nodePath);
        if (client.checkExists().forPath(nodePath) != null) {
            client.delete().forPath(nodePath);
        }

        thisNode = null;
    }

    private void updateNodeStateAndSleep(String path) throws Exception {
        byte[] bytes = client.getData().forPath(path);
        Node downNode = JsonFeatureMapper.deserialize(bytes, Node.class);
        downNode.setState(DOWN);
        client.setData().forPath(path, JsonFeatureMapper.serialize(downNode));

        readyNodes.put(downNode.getId(), downNode);
        if (configuration.getShutdownMaxWaitTimeMilliseconds() > 0) {
            TimeUnit.MILLISECONDS.sleep(configuration.getShutdownMaxWaitTimeMilliseconds());
        }
    }

    @Override
    public List<Node> getClusterNodes() {
        return new ArrayList<>(readyNodes.values());
    }

    @Override
    public List<Node> getClusterReadyNodes() {
        return readyNodes.values().stream()
                .filter(node -> UP.equals(node.getState())).collect(Collectors.toList());
    }

    @Override
    public Node getClusterReadyNode(String id) {
        return readyNodes.get(id).getState().equals(UP) ? readyNodes.get(id) : null;
    }

    @Override
    public Node getThisNode() {
        return thisNode;
    }

    @Override
    public void shutdown() throws Exception {
        cache.close();
    }

    @Override
    public String getController() throws Exception {
        return latch.getLeader().getId();
    }

    @Override
    public boolean isController() {
        return latch.hasLeadership();
    }

    @Override
    public void addClusterListener(ClusterListener listener) {
        listeners.add(listener);
    }

    @Override
    public String getClusterName() {
        return configuration.getClusterName();
    }
}
