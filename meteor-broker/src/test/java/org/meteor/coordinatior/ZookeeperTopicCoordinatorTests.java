package org.meteor.coordinatior;

import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.meteor.common.message.PartitionInfo;
import org.meteor.common.message.TopicPartition;
import org.meteor.config.ServerConfig;
import org.meteor.internal.CorrelationIdConstants;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class ZookeeperTopicCoordinatorTests {
    private TopicCoordinator coordinator;
    private TestingServer server;
    private final String topic = "test";
    private final int partitions = 1;
    private final int replicas = 1;

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
        DefaultCoordinator defaultCoordinator = new DefaultCoordinator(config);
        defaultCoordinator.start();
        coordinator = defaultCoordinator.getTopicCoordinator();
        //only fot unit test: wait to cluster register test node
        TimeUnit.SECONDS.sleep(5);
    }

    @Test
    public void testTopicPattern() throws Exception {
        Pattern pattern = Pattern.compile("^[\\w\\-#]+$");
        Assert.assertTrue(pattern.matcher("test").matches());
        Assert.assertTrue(pattern.matcher("001").matches());
        Assert.assertTrue(pattern.matcher("100").matches());
        Assert.assertTrue(pattern.matcher("test-1").matches());
        Assert.assertTrue(pattern.matcher("test_1").matches());
        Assert.assertTrue(pattern.matcher("test-1_1").matches());
        Assert.assertTrue(pattern.matcher("test-1_1").matches());
        Assert.assertTrue(pattern.matcher("test#001").matches());
        Assert.assertTrue(pattern.matcher("test-#_001").matches());
        Assert.assertFalse(pattern.matcher("test/001").matches());
        Assert.assertFalse(pattern.matcher("test\\001").matches());
        Assert.assertFalse(pattern.matcher("test&001").matches());
        Assert.assertFalse(pattern.matcher("test&%001").matches());
        Assert.assertFalse(pattern.matcher("test&$001").matches());
        Assert.assertFalse(pattern.matcher("test&@001").matches());
        Assert.assertFalse(pattern.matcher("test 001").matches());
    }

    @Test
    public void testCrateTopic() throws Exception {
        Map<String, Object> results = coordinator.createTopic(topic, partitions, replicas, null);
        Assertions.assertEquals(results.size(), 2);
        Assertions.assertNotNull(results.get(CorrelationIdConstants.TOPIC_ID));
        Assertions.assertNotNull(results.get(CorrelationIdConstants.PARTITION_REPLICAS));

        Set<PartitionInfo> topicInfos = coordinator.getTopicInfo(topic);
        Assertions.assertNotNull(topicInfos);
        Assertions.assertEquals(topicInfos.size(), 1);
        Iterator<PartitionInfo> iterator = topicInfos.iterator();
        PartitionInfo partitionInfo = iterator.next();
        Assertions.assertEquals(partitionInfo.getTopicId(), results.get(CorrelationIdConstants.TOPIC_ID));
        Assertions.assertEquals(partitionInfo.getTopic(), topic);
    }

    @Test
    public void testDeleteTopic() throws Exception {
        Map<String, Object> results = coordinator.createTopic(topic, partitions, replicas, null);
        Assertions.assertEquals(results.size(), 2);
        Assertions.assertNotNull(results.get(CorrelationIdConstants.TOPIC_ID));
        Assertions.assertNotNull(results.get(CorrelationIdConstants.PARTITION_REPLICAS));

        Set<PartitionInfo> topicInfos = coordinator.getTopicInfo(topic);
        Assertions.assertNotNull(topicInfos);
        Assertions.assertEquals(topicInfos.size(), 1);
        Iterator<PartitionInfo> iterator = topicInfos.iterator();
        PartitionInfo partitionInfo = iterator.next();
        Assertions.assertEquals(partitionInfo.getTopicId(), results.get(CorrelationIdConstants.TOPIC_ID));
        Assertions.assertEquals(partitionInfo.getTopic(), topic);

        coordinator.deleteTopic(topic);
        Set<String> allTopics = coordinator.getAllTopics();
        Assertions.assertNotNull(allTopics);
        Assertions.assertEquals(allTopics.size(), 0);
    }

    @Test
    public void testGetAllTopics() throws Exception {
        Map<String, Object> results = coordinator.createTopic(topic, partitions, replicas, null);
        Assertions.assertEquals(results.size(), 2);
        Assertions.assertNotNull(results.get(CorrelationIdConstants.TOPIC_ID));
        Assertions.assertNotNull(results.get(CorrelationIdConstants.PARTITION_REPLICAS));

        Set<PartitionInfo> topicInfos = coordinator.getTopicInfo(topic);
        Assertions.assertNotNull(topicInfos);
        Assertions.assertEquals(topicInfos.size(), 1);
        Iterator<PartitionInfo> iterator = topicInfos.iterator();
        PartitionInfo partitionInfo = iterator.next();
        Assertions.assertEquals(partitionInfo.getTopicId(), results.get(CorrelationIdConstants.TOPIC_ID));
        Assertions.assertEquals(partitionInfo.getTopic(), topic);
        Set<String> allTopics = coordinator.getAllTopics();
        Assertions.assertNotNull(allTopics);
        Assertions.assertEquals(allTopics.size(), 1);
        String result = allTopics.iterator().next();
        Assertions.assertNotNull(result);
        Assertions.assertEquals(result, topic);
    }

    @Test
    public void testGetPartitionInfo() throws Exception {
        Map<String, Object> results = coordinator.createTopic(topic, partitions, replicas, null);
        Assertions.assertEquals(results.size(), 2);
        Assertions.assertNotNull(results.get(CorrelationIdConstants.TOPIC_ID));
        Assertions.assertNotNull(results.get(CorrelationIdConstants.PARTITION_REPLICAS));

        TopicPartition topicPartition = new TopicPartition(topic, 0);
        PartitionInfo partitionInfo = coordinator.getPartitionInfo(topicPartition);
        Assertions.assertNotNull(partitionInfo);
        Assertions.assertEquals(partitionInfo.getTopicId(), results.get(CorrelationIdConstants.TOPIC_ID));
        Assertions.assertEquals(partitionInfo.getTopic(), topic);
    }

    @After
    public void tearDown() throws Exception {
        coordinator.shutdown();
        server.close();
    }
}
