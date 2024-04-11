package org.meteor.coordinator;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.meteor.common.message.Node;
import org.meteor.config.ServerConfig;
import org.meteor.support.ClusterManager;
import org.meteor.support.DefaultMeteorManager;

public class ZookeeperClusterCoordinatorTest {
    private TestingServer server;
    private ClusterManager coordinator;

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
        DefaultMeteorManager defaultCoordinator = new DefaultMeteorManager(config);
        coordinator = defaultCoordinator.getClusterCoordinator();
        coordinator.start();
        // only for unit test: wait to custer register node
        TimeUnit.SECONDS.sleep(5);
    }

    @Test
    public void testGetReadyNode() throws Exception {
        List<Node> clusterReadyNodes = coordinator.getClusterReadyNodes();
        Assertions.assertNotNull(clusterReadyNodes);
        Assertions.assertEquals(clusterReadyNodes.size(), 1);
        Assertions.assertEquals(clusterReadyNodes.get(0).getCluster(), "default");
        Assertions.assertEquals(clusterReadyNodes.get(0).getId(), "default");
        Assertions.assertEquals(clusterReadyNodes.get(0).getState(), "UP");
    }

    @Test
    public void testGetReadyNodes() throws Exception {
        List<Node> clusterNodes = coordinator.getClusterNodes();
        Assertions.assertNotNull(clusterNodes);
        Assertions.assertEquals(clusterNodes.size(), 1);
        Assertions.assertEquals(clusterNodes.get(0).getCluster(), "default");
        Assertions.assertEquals(clusterNodes.get(0).getId(), "default");
        Assertions.assertEquals(clusterNodes.get(0).getState(), "UP");
    }

    @Test
    public void testGetThisNode() throws Exception {
        Node thisNode = coordinator.getThisNode();
        Assertions.assertNotNull(thisNode);
        Assertions.assertEquals(thisNode.getCluster(), "default");
        Assertions.assertEquals(thisNode.getId(), "default");
        Assertions.assertEquals(thisNode.getState(), "UP");
    }

    @After
    public void shutdown() throws Exception {
        coordinator.shutdown();
        server.close();
    }
}
