package org.meteor.support;

import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.meteor.common.message.Node;
import org.meteor.config.ServerConfig;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ZookeeperClusterManagerTest {
    /**
     * A TestingServer instance used for managing an in-memory ZooKeeper server
     * during unit tests. This server provides a ZooKeeper environment for the
     * tests to interact with, allowing for the testing of ZooKeeper related
     * operations without requiring a real ZooKeeper server.
     */
    private TestingServer server;
    /**
     * Manages the operations and lifecycle of a cluster of nodes.
     * This instance is used for starting the cluster, retrieving node information,
     * and shutting down the cluster.
     */
    private ClusterManager clusterManager;

    /**
     * Sets up the testing environment for Zookeeper-based cluster management.
     * <p>
     * This method performs the following initializations and configurations:
     * - Instantiates a TestingServer for Zookeeper.
     * - Configures Zookeeper-related properties such as connection URL, retry settings, and timeouts.
     * - Initializes a ServerConfig object with the specified properties.
     * - Creates a DefaultMeteorManager instance using the ServerConfig.
     * - Retrieves the ClusterManager from the DefaultMeteorManager and starts it.
     * - Pauses execution briefly to allow the cluster to register nodes.
     *
     * @throws Exception if any errors occur during the setup process.
     */
    @Before
    public void setUp() throws Exception {
        server = new TestingServer();
        Properties properties = new Properties();
        properties.put("zookeeper.url", server.getConnectString());
        properties.put("zookeeper.connection.retry.sleep.milliseconds", 3000);
        properties.put("zookeeper.connection.retries", 3);
        properties.put("zookeeper.connection.timeout.milliseconds", 3000);
        properties.put("zookeeper.session.timeout.milliseconds", 30000);
        ServerConfig config = new ServerConfig(properties);
        DefaultMeteorManager defaultMeteorManager = new DefaultMeteorManager(config);
        clusterManager = defaultMeteorManager.getClusterManager();
        clusterManager.start();
        // only for unit test: wait to custer register node
        TimeUnit.SECONDS.sleep(5);
    }

    /**
     * Tests the retrieval of a ready node from the cluster.
     * The method ensures that the list of ready nodes in the cluster contains
     * exactly one node, and that this node has the expected cluster, ID, and state.
     *
     * @throws Exception if there is an error during the execution of the test.
     */
    @Test
    public void testGetReadyNode() throws Exception {
        List<Node> clusterReadyNodes = clusterManager.getClusterReadyNodes();
        Assertions.assertNotNull(clusterReadyNodes);
        Assertions.assertEquals(1, clusterReadyNodes.size());
        Assertions.assertEquals("default", clusterReadyNodes.getFirst().getCluster());
        Assertions.assertEquals("default", clusterReadyNodes.getFirst().getId());
        Assertions.assertEquals("UP", clusterReadyNodes.getFirst().getState());
    }

    /**
     * Tests if the cluster manager correctly retrieves the nodes in the cluster and validates their
     * properties such as cluster name, node ID, and state.
     *
     * @throws Exception if an error occurs while retrieving the cluster nodes
     */
    @Test
    public void testGetReadyNodes() throws Exception {
        List<Node> clusterNodes = clusterManager.getClusterNodes();
        Assertions.assertNotNull(clusterNodes);
        Assertions.assertEquals(1, clusterNodes.size());
        Assertions.assertEquals("default", clusterNodes.getFirst().getCluster());
        Assertions.assertEquals("default", clusterNodes.getFirst().getId());
        Assertions.assertEquals("UP", clusterNodes.getFirst().getState());
    }

    /**
     * Tests the retrieval of the current node in the cluster.
     *
     * @throws Exception if there is an error during the test execution.
     * <p>
     * This method retrieves the current node from the cluster using the ClusterManager.
     * It performs the following validations:
     * - The retrieved node is not null.
     * - The cluster name of the node matches the expected value "default".
     * - The ID of the node matches the expected value "default".
     * - The state of the node is "UP".
     *
     * This ensures that the current node is correctly identified and has the expected properties.
     */
    @Test
    public void testGetThisNode() throws Exception {
        Node thisNode = clusterManager.getThisNode();
        Assertions.assertNotNull(thisNode);
        Assertions.assertEquals("default", thisNode.getCluster());
        Assertions.assertEquals("default", thisNode.getId());
        Assertions.assertEquals("UP", thisNode.getState());
    }

    /**
     * Shuts down the cluster manager and the testing server. This method ensures that
     * all resources associated with the cluster manager and server are properly
     * terminated and cleaned up after the execution of tests.
     *
     * @throws Exception if an error occurs during the shutdown process.
     */
    @After
    public void shutdown() throws Exception {
        clusterManager.shutdown();
        server.close();
    }
}
