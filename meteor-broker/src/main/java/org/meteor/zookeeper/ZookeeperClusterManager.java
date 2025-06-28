package org.meteor.zookeeper;

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
import org.meteor.support.ConsistentHashingRing;
import org.meteor.support.SerializeFeatureSupport;
import org.meteor.listener.ClusterListener;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.meteor.common.message.NodeState.DOWN;
import static org.meteor.common.message.NodeState.UP;

/**
 * Manages a cluster of nodes with Zookeeper for coordination. This class handles
 * the addition, removal, and state changes of nodes in the cluster, as well as leader election.
 */
public class ZookeeperClusterManager implements ClusterManager {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ZookeeperClusterManager.class);
    /**
     * A list of ClusterListener instances that are associated with the
     * ZookeeperClusterManager. These listeners are notified of various
     * cluster-related events, such as changes in node states and roles.
     */
    protected final List<ClusterListener> listeners = new ObjectArrayList<>();
    /**
     * Common configuration object holding various settings for the ZookeeperClusterManager.
     * <p>
     * This field is initialized during the creation of ZookeeperClusterManager and
     * provides access to configuration properties such as server ID, cluster name,
     * advertised address and port, and other related settings. The CommonConfig class
     * facilitates organized and centralized management of configuration properties
     * which are essential for the manager's operation.
     */
    private final CommonConfig configuration;
    /**
     * A thread-safe map that holds nodes which are ready for use in the ZookeeperClusterManager.
     * The keys are node identifiers, and the values are Node instances representing the ready nodes.
     * This map is used to quickly access nodes that are in a ready state without traversing the entire cluster.
     */
    private final Map<String, Node> readyNodes = new ConcurrentHashMap<>();
    /**
     * The consistent hashing ring used within the ZookeeperClusterManager for distributing
     * keys and managing load balancing across multiple nodes.
     * This is implemented using the ConsistentHashingRing class which maintains a ring
     * structure with virtual nodes to ensure even distribution and redundancy.
     */
    private final ConsistentHashingRing hashingRing;
    /**
     * A protected instance of CuratorFramework used for managing
     * interactions with a Zookeeper cluster. This client is responsible
     * for creating, deleting, reading, and writing to Zookeeper nodes.
     */
    protected CuratorFramework client;
    /**
     * A listener for monitoring connection state changes to the Zookeeper cluster.
     * This listener is used within the context of managing cluster nodes and their states,
     * reacting to connectivity events to maintain the cluster's stability and reliability.
     */
    protected ConnectionStateListener connectionStateListener;
    /**
     * The `cache` variable is an instance of CuratorCache that is used to maintain and manage a local
     * cache of the znodes in the Zookeeper cluster. This cache helps in reducing the number of
     * read operations performed on the Zookeeper server by caching the data locally and keeping
     * it synchronized with the server. Operations on the cache are reflected in the local data
     * which provides faster read access and offloads the Zookeeper server from frequent read
     * requests.
     */
    protected CuratorCache cache;
    /**
     * Indicates whether the current node is registered in the Zookeeper cluster.
     * This volatile boolean ensures visibility and thread-safety for changes made by different threads.
     */
    private volatile boolean registered = false;
    /**
     * Maintains the state of leadership election in the Zookeeper cluster. This
     * latch is used to determine and elect the leader node among the cluster
     * nodes, ensuring a single node acts as the controller at any given time.
     */
    private LeaderLatch latch;
    /**
     * Represents the current node within the cluster managed by the ZookeeperClusterManager.
     * This variable holds the reference to the Node instance that denotes
     * the present node's identity and state within the cluster.
     */
    private Node thisNode;

    /**
     * Constructs a ZookeeperClusterManager with the provided server configuration and consistent hashing ring.
     *
     * @param config The configuration settings for the server, including Zookeeper specific settings.
     * @param hashingRing An instance of ConsistentHashingRing used for node distribution and lookup.
     */
    public ZookeeperClusterManager(ServerConfig config, ConsistentHashingRing hashingRing) {
        this.configuration = config.getCommonConfig();
        this.client = ZookeeperClientFactory.getReadyClient(config.getZookeeperConfig(), config.getCommonConfig().getClusterName());
        this.hashingRing = hashingRing;
    }

    /**
     * Starts the ZookeeperClusterManager by setting up the necessary listeners
     * and registering the node in the ZooKeeper ensemble.
     *
     * @throws Exception if any error occurs during the start-up process.
     *
     * <p>This method performs the following actions:</p>
     * <ul>
     *   <li>Initializes a connection state listener to handle the reconnected state.</li>
     *   <li>On reconnection, it checks if the broker ID exists and attempts to re-register if not.</li>
     *   <li>Adds the connection state listener to the client.</li>
     *   <li>Starts the brokers listener to listen for changes in broker nodes.</li>
     *   <li>Elects a controller for the cluster.</li>
     *   <li>Registers the current node in the cluster.</li>
     * </ul>
     */
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

    /**
     * Initiates the election process for determining the controller in the cluster.
     * This method uses a LeaderLatch to participate in the leader election process.
     * Upon becoming the leader, it notifies all registered listeners by invoking their
     * {@code onGetControlRole} method. If the node loses its leadership, it triggers
     * the {@code onLostControlRole} method for all listeners.
     *
     * @throws Exception if an error occurs while starting the LeaderLatch
     */
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

    /**
     * Starts a listener on the given path to monitor the broker nodes in the cluster.
     * This listener handles node additions, removals, and updates by reacting to the respective events.
     *
     * @param path the Zookeeper path to monitor for child node events.
     */
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
                        Node node = SerializeFeatureSupport.deserialize(data.getData(), Node.class);

                        readyNodes.put(node.getId(), node);
                        for (ClusterListener listener : listeners) {
                            listener.onNodeJoin(node);
                            if (hashingRing != null) {
                                hashingRing.insertNode(node.getId());
                            }
                        }
                    }

                    private void handleRemove(PathChildrenCacheEvent event) throws Exception {
                        ChildData data = event.getData();
                        Node node = SerializeFeatureSupport.deserialize(data.getData(), Node.class);

                        readyNodes.remove(node.getId());
                        for (ClusterListener listener : listeners) {
                            listener.onNodeLeave(node);
                            if (hashingRing != null) {
                                hashingRing.deleteNode(node.getId());
                            }
                        }
                    }

                    private void handlerUpdated(PathChildrenCacheEvent event) throws Exception {
                        ChildData data = event.getData();
                        Node node = SerializeFeatureSupport.deserialize(data.getData(), Node.class);

                        readyNodes.put(node.getId(), node);
                        if (DOWN.equals(node.getState())) {
                            for (ClusterListener listener : listeners) {
                                listener.onNodeDown(node);
                                if (hashingRing != null) {
                                    hashingRing.deleteNode(node.getId());
                                }
                            }
                        }
                    }
                })
                .build();

        cache.listenable().addListener(listener);
        cache.start();
    }

    /**
     * Registers a node in the Zookeeper cluster at the specified path.
     *
     * @param path the Zookeeper path where the node should be registered.
     * @throws Exception if there is an error during the registration process.
     */
    protected void registerNode(String path) throws Exception {
        CreateBuilder createBuilder = client.create();
        thisNode = new Node(configuration.getServerId(), configuration.getAdvertisedAddress(), configuration.getAdvertisedPort(),
                System.currentTimeMillis(), configuration.getClusterName(), UP);

        try {
            createBuilder.creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
                    .forPath(String.format(path, configuration.getServerId()),
                            SerializeFeatureSupport.serialize(thisNode));
            registered = true;
        } catch (KeeperException.NodeExistsException e) {
            throw new KeeperException.NodeExistsException(String.format("Server id[%s] should be unique", configuration.getServerId()));
        }
    }

    /**
     * Unregisters a node from the cluster.
     *
     * @param path The path template of the node to be unregistered. This path will be
     *             formatted with the server ID to determine the full path of the node.
     * @throws Exception if any error occurs during the unregistration process.
     */
    protected void unregistered(String path) throws Exception {
        if (client == null || !registered) {
            if (logger.isDebugEnabled()) {
                logger.debug("Unregister node failed");
            }
            return;
        }

        String nodePath = String.format(path, configuration.getServerId());
        updateNodeStateAndSleep(nodePath);
        if (client.checkExists().forPath(nodePath) != null) {
            client.delete().forPath(nodePath);
        }

        thisNode = null;
    }

    /**
     * Updates the state of a node to DOWN and sleeps for the configured maximum wait time.
     *
     * @param path the ZooKeeper path to the node's data
     * @throws Exception if any error occurs during the node state update or sleep operation
     */
    private void updateNodeStateAndSleep(String path) throws Exception {
        byte[] bytes = client.getData().forPath(path);
        Node downNode = SerializeFeatureSupport.deserialize(bytes, Node.class);
        downNode.setState(DOWN);
        client.setData().forPath(path, SerializeFeatureSupport.serialize(downNode));

        readyNodes.put(downNode.getId(), downNode);
        if (configuration.getShutdownMaxWaitTimeMilliseconds() > 0) {
            TimeUnit.MILLISECONDS.sleep(configuration.getShutdownMaxWaitTimeMilliseconds());
        }
    }

    /**
     * Retrieves a list of all nodes that are currently ready in the cluster.
     * The list is created based on the `readyNodes` map, which maintains the
     * current state of nodes that are available and ready for operations.
     *
     * @return a list of Node objects representing all nodes in the ready state within the cluster
     */
    @Override
    public List<Node> getClusterNodes() {
        return new ArrayList<>(readyNodes.values());
    }

    /**
     * Retrieves a list of nodes that are ready and available in the cluster.
     *
     * @return a list of nodes that have a state indicating they are ready for use in the cluster.
     */
    @Override
    public List<Node> getClusterReadyNodes() {
        return readyNodes.values().stream()
                .filter(node -> UP.equals(node.getState())).collect(Collectors.toList());
    }

    /**
     * Retrieves a node in the cluster that is in a ready state based on the given node identifier.
     *
     * @param id the identifier of the node to be retrieved
     * @return the node that matches the provided identifier and is in a ready state, or null if no such node is found
     */
    @Override
    public Node getClusterReadyNode(String id) {
        return readyNodes.get(id).getState().equals(UP) ? readyNodes.get(id) : null;
    }

    /**
     * Retrieves this node instance managed by the ZookeeperClusterManager.
     *
     * @return the current Node instance representing this node within the cluster.
     */
    @Override
    public Node getThisNode() {
        return thisNode;
    }

    /**
     * Shuts down the ZookeeperClusterManager and releases associated resources.
     * <p>
     * This method performs the following actions:
     * - Closes the cache to release any resources it holds.
     * - Deletes the current node from the consistent hashing ring to update the cluster state.
     *
     * @throws Exception if an error occurs during the shutdown process.
     */
    @Override
    public void shutdown() throws Exception {
        cache.close();
        hashingRing.deleteNode(thisNode.getId());
    }

    /**
     * Retrieves the identifier of the current controller node.
     *
     * @return A string representing the identifier of the controller node.
     * @throws Exception If an error occurs while trying to retrieve the controller.
     */
    @Override
    public String getController() throws Exception {
        return latch.getLeader().getId();
    }

    /**
     * Checks if the current node has the controller role in the cluster.
     *
     * @return true if the current node is the controller; false otherwise.
     */
    @Override
    public boolean isController() {
        return latch.hasLeadership();
    }

    /**
     * Adds a ClusterListener to the ZookeeperClusterManager instance. This listener
     * will be notified of various cluster-related events such as nodes joining or
     * leaving the cluster, and nodes changing their operational state.
     *
     * @param listener The ClusterListener instance that will handle the cluster-related events.
     */
    @Override
    public void addClusterListener(ClusterListener listener) {
        listeners.add(listener);
    }

    /**
     * Retrieves the name of the cluster.
     *
     * @return the name of the cluster
     */
    @Override
    public String getClusterName() {
        return configuration.getClusterName();
    }
}
