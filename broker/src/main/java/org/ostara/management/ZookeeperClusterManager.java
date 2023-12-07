package org.ostara.management;

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
import org.ostara.common.Node;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.core.CoreConfig;
import org.ostara.listener.ClusterListener;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ZookeeperClusterManager implements ClusterManager {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ZookeeperClusterManager.class);
    private static final String UP = "up";
    private static final String DOWN = "down";
    protected final CoreConfig config;
    protected final List<ClusterListener> listeners = new LinkedList<>();
    private final Map<String, Node> activeNodes = new ConcurrentHashMap<>();
    protected final CuratorFramework client;
    private volatile boolean registered = false;
    protected ConnectionStateListener connectionStateListener;
    protected CuratorCache cache;
    private LeaderLatch latch;
    private Node thisNode;

    public ZookeeperClusterManager(CoreConfig config) {
        this.config = config;
        this.client = ZookeeperClient.getClient(config, config.getClusterName());
    }

    @Override
    public void start() throws Exception {
        connectionStateListener = (client, state) -> {
            if (state == ConnectionState.RECONNECTED) {
                try {
                    Stat stat = client.checkExists().forPath(String.format(PathConstants.BROKERS_ID, config.getServerId()));
                    if (stat != null) {
                        return;
                    }

                    registerNode(PathConstants.BROKERS_ID);
                } catch (Exception e) {
                    logger.error("Re-register node failed", e);
                }
            }
        };

        client.getConnectionStateListenable().addListener(connectionStateListener);
        startBrokersListener(PathConstants.BROKERS_IDS);
        electController();
        registerNode(PathConstants.BROKERS_ID);
    }

    private void electController() throws Exception {
        latch = new LeaderLatch(client, PathConstants.CONTROLLER, config.getServerId(), LeaderLatch.CloseMode.NOTIFY_LEADER);
        latch.addListener(new LeaderLatchListener() {
            @Override
            public void isLeader() {
                logger.info("Get the controller role");
                for (ClusterListener listener : listeners) {
                    listener.onGetControlRole(thisNode);
                }
            }

            @Override
            public void notLeader() {
                logger.info("Lost the controller role");
                for (ClusterListener listener : listeners) {
                    listener.onLostControlRole(thisNode);
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
                            }
                        }
                    }


                    private void handleAdd(PathChildrenCacheEvent event) throws Exception {
                        ChildData data = event.getData();
                        Node node = JsonMapper.deserialize(data.getData(), Node.class);

                        activeNodes.put(node.getId(), node);
                        for (ClusterListener listener : listeners) {
                            listener.onNodeJoin(node);
                        }
                    }

                    private void handleRemove(PathChildrenCacheEvent event) throws Exception {
                        ChildData data = event.getData();
                        Node node = JsonMapper.deserialize(data.getData(), Node.class);

                        activeNodes.remove(node.getId());
                        for (ClusterListener listener : listeners) {
                            listener.onNodeLeave(node);
                        }
                    }

                    private void handlerUpdated(PathChildrenCacheEvent event) throws Exception {
                        ChildData data = event.getData();
                        Node node = JsonMapper.deserialize(data.getData(), Node.class);

                        activeNodes.put(node.getId(), node);
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
        thisNode = new Node(config.getServerId(), config.getAdvertisedAddress(), config.getAdvertisedPort(),
                System.currentTimeMillis(), config.getClusterName(), UP);

        try {
            createBuilder.creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
                    .forPath(String.format(path, config.getServerId()), JsonMapper.serialize(thisNode));

            registered = true;
        } catch (KeeperException.NodeExistsException e) {
            throw new RuntimeException(String.format("Server id[%s] should be unique", config.getServerId()));
        }
    }

    protected void unregistered(String path) throws Exception {
        if (client == null || !registered) {
            return;
        }

        String nodePath = String.format(path, config.getServerId());
        updateNodeStateAndSleep(nodePath);
        if (client.checkExists().forPath(nodePath) != null) {
            client.delete().forPath(nodePath);
        }

        thisNode = null;
    }

    private void updateNodeStateAndSleep(String path) throws Exception {
        byte[] bytes = client.getData().forPath(path);
        Node downNode = JsonMapper.deserialize(bytes, Node.class);
        downNode.setState(DOWN);
        client.setData().forPath(path, JsonMapper.serialize(downNode));

        activeNodes.put(downNode.getId(), downNode);
        if (config.getShutdownMaxWaitTimeMs() > 0) {
            TimeUnit.MILLISECONDS.sleep(config.getShutdownMaxWaitTimeMs());
        }
    }

    @Override
    public List<Node> getClusterNodes() {
        return new ArrayList<>(activeNodes.values());
    }

    @Override
    public List<Node> getClusterUpNodes() {
        return activeNodes.values().stream()
                .filter(node -> UP.equals(node.getState())).collect(Collectors.toList());
    }

    @Override
    public Node getClusterNode(String id) {
        return activeNodes.get(id).getState().equals(UP) ? activeNodes.get(id) : null;
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
        return config.getClusterName();
    }
}
