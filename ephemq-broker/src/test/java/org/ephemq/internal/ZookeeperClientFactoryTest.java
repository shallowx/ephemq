package org.ephemq.internal;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.ephemq.config.ZookeeperConfig;
import org.ephemq.zookeeper.ZookeeperClientFactory;

import java.util.Properties;

public class ZookeeperClientFactoryTest {
    /**
     * A TestingServer instance used to simulate a real Zookeeper server for testing purposes.
     * This in-memory server is used to test Zookeeper client interactions without requiring an actual Zookeeper server instance.
     */
    private TestingServer server;

    /**
     * Sets up the testing environment before each test case is executed.
     * Initializes the embedded Zookeeper testing server.
     *
     * @throws Exception if there is an issue starting the testing server.
     */
    @Before
    public void setUp() throws Exception {
        server = new TestingServer();
    }

    /**
     * Tests the creation and initialization of a new Zookeeper client.
     * <p>
     * This test verifies that a Zookeeper client can be correctly configured and connected to a Zookeeper server.
     * It creates a Zookeeper configuration using predefined properties, initializes a client with that configuration,
     * and confirms that the client is not null.
     * Additionally, it performs a transaction operation to create an ephemeral node in Zookeeper
     * and verifies that the operation is successful.
     *
     * @throws Exception if any error occurs during the test execution.
     */
    @Test
    public void testNewClient() throws Exception {
        Properties pro = new Properties();
        pro.put("zookeeper.url", server.getConnectString());
        pro.put("zookeeper.connection.retry.sleep.milliseconds", 3000);
        pro.put("zookeeper.connection.retries", 3);
        pro.put("zookeeper.connection.timeout.milliseconds", 3000);
        pro.put("zookeeper.session.timeout.milliseconds", 30000);
        ZookeeperConfig config = new ZookeeperConfig(pro);
        CuratorFramework readyClient = ZookeeperClientFactory.getReadyClient(config, "test-cluster-name");
        Assertions.assertNotNull(readyClient);

        CuratorOp curatorOp = readyClient.transactionOp().create().withMode(CreateMode.EPHEMERAL).forPath("/test/cluster");
        Assertions.assertNotNull(curatorOp);
        Assertions.assertEquals("/test/cluster", curatorOp.getTypeAndPath().getForPath());
    }

    /**
     * Shuts down the TestingServer instance after each test.
     * <p>
     * This method is annotated with @After which indicates that it is a teardown method
     * to be executed after each test in the test class. It ensures that the TestingServer
     * is properly closed and resources are released.
     *
     * @throws Exception if an error occurs while closing the TestingServer.
     */
    @After
    public void tearDown() throws Exception {
        server.close();
    }
}
