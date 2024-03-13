package org.meteor.internal;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.meteor.config.ZookeeperConfig;

import java.util.Properties;

public class ZookeeperClientFactoryTests {

    private TestingServer server;

    @Before
    public void setUp() throws Exception {
        server = new TestingServer();
    }

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
        Assertions.assertEquals(curatorOp.getTypeAndPath().getForPath(), "/test/cluster");
    }

    @After
    public void tearDown() throws Exception {
        server.close();
    }
}
