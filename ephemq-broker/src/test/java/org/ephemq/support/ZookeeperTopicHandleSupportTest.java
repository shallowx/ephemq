package org.ephemq.support;

import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.ephemq.common.message.PartitionInfo;
import org.ephemq.common.message.TopicPartition;
import org.ephemq.config.ServerConfig;
import org.ephemq.zookeeper.CorrelationIdConstants;
import org.ephemq.zookeeper.TopicHandleSupport;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class ZookeeperTopicHandleSupportTest {
    /**
     * The name of the topic used in the test cases within ZookeeperTopicHandleSupportTest class.
     * This is a static final string representing a predefined topic name "test".
     */
    private final String topic = "test";
    /**
     * Represents the number of partitions in a topic used in Zookeeper-related tests.
     */
    private final int partitions = 1;
    /**
     * The default number of replicas for a topic partition in the test scenario.
     */
    private final int replicas = 1;
    /**
     * An instance of TopicHandleSupport that provides necessary methods and operations to manage and handle
     * topics and their partitions within the ZookeeperTopicHandleSupportTest class.
     * The support variable is a key component in facilitating topic creation, deletion, partition management,
     * and other related operations within the test environment.
     */
    private TopicHandleSupport support;
    /**
     * The `server` variable represents an instance of {@link TestingServer}.
     * It is used to simulate a ZooKeeper server for testing purposes
     * within the {@code ZookeeperTopicHandleSupportTest} class.
     */
    private TestingServer server;

    /**
     * Sets up the testing environment before each test case is executed.
     * <p>
     * This method initializes a {@link TestingServer} instance to simulate a Zookeeper server
     * and configures the server properties needed for testing. The method then initializes
     * the {@link DefaultMeteorManager} with the configured properties and starts it,
     * allowing test cases to interact with a mock topic handle support system.
     *
     * @throws Exception if any error occurs during the setup process, such as an issue with
     *                   the TestingServer or the DefaultMeteorManager initialization.
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
        defaultMeteorManager.start();
        support = defaultMeteorManager.getTopicHandleSupport();
        //only fot unit test: wait to cluster register test node
        TimeUnit.SECONDS.sleep(5);
    }

    /**
     * Tests the validity of various topic names against a predefined pattern.
     * The pattern allows alphanumeric characters, underscores, dashes, and hash symbols.
     * Any other characters should invalidate the topic name.
     *
     * @throws Exception if an unexpected error occurs during the pattern matching.
     */
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

    /**
     * Tests the creation of a topic in a Zookeeper-backed environment.
     * <p>
     * This test verifies that a topic with specified partitions and replicas can be
     * created successfully, and that the resulting metadata includes valid topic ID
     * and partition replicas information.
     *
     * @throws Exception If an error occurs during topic creation or fetching topic information.
     */
    @Test
    public void testCrateTopic() throws Exception {
        Map<String, Object> results = support.createTopic(topic, partitions, replicas, null);
        Assertions.assertEquals(2, results.size());
        Assertions.assertNotNull(results.get(CorrelationIdConstants.TOPIC_ID));
        Assertions.assertNotNull(results.get(CorrelationIdConstants.PARTITION_REPLICAS));

        Set<PartitionInfo> topicInfos = support.getTopicInfo(topic);
        Assertions.assertNotNull(topicInfos);
        Assertions.assertEquals(1, topicInfos.size());
        Iterator<PartitionInfo> iterator = topicInfos.iterator();
        PartitionInfo partitionInfo = iterator.next();
        Assertions.assertEquals(partitionInfo.getTopicId(), results.get(CorrelationIdConstants.TOPIC_ID));
        Assertions.assertEquals(partitionInfo.getTopic(), topic);
    }

    /**
     * Tests the deletion of a Kafka topic.
     * <p>
     * This method validates the following sequence:
     * 1. Creates a new topic using specified configurations.
     * 2. Verifies the creation by checking the returned topic details.
     * 3. Confirms the existence of the topic via partition information.
     * 4. Deletes the topic.
     * 5. Validates that the topic is successfully deleted by verifying the absence of the topic.
     *
     * @throws Exception if an error occurs during the topic creation, retrieval, or deletion process.
     */
    @Test
    public void testDeleteTopic() throws Exception {
        Map<String, Object> results = support.createTopic(topic, partitions, replicas, null);
        Assertions.assertEquals(2, results.size());
        Assertions.assertNotNull(results.get(CorrelationIdConstants.TOPIC_ID));
        Assertions.assertNotNull(results.get(CorrelationIdConstants.PARTITION_REPLICAS));

        Set<PartitionInfo> topicInfos = support.getTopicInfo(topic);
        Assertions.assertNotNull(topicInfos);
        Assertions.assertEquals(1, topicInfos.size());
        Iterator<PartitionInfo> iterator = topicInfos.iterator();
        PartitionInfo partitionInfo = iterator.next();
        Assertions.assertEquals(partitionInfo.getTopicId(), results.get(CorrelationIdConstants.TOPIC_ID));
        Assertions.assertEquals(partitionInfo.getTopic(), topic);

        support.deleteTopic(topic);
        Set<String> allTopics = support.getAllTopics();
        Assertions.assertNotNull(allTopics);
        Assertions.assertEquals(0, allTopics.size());
    }

    /**
     * Tests the functionality of retrieving all topics in the system.
     * <p>
     * This test validates that:
     * 1. A topic can be successfully created with the specified configurations.
     * 2. The topic information, including topic ID and partition replicas, is correctly stored.
     * 3. The topic retrieval method returns accurate information regarding existing topics.
     *
     * @throws Exception if there is an error during topic creation or retrieval.
     */
    @Test
    public void testGetAllTopics() throws Exception {
        Map<String, Object> results = support.createTopic(topic, partitions, replicas, null);
        Assertions.assertEquals(2, results.size());
        Assertions.assertNotNull(results.get(CorrelationIdConstants.TOPIC_ID));
        Assertions.assertNotNull(results.get(CorrelationIdConstants.PARTITION_REPLICAS));

        Set<PartitionInfo> topicInfos = support.getTopicInfo(topic);
        Assertions.assertNotNull(topicInfos);
        Assertions.assertEquals(1, topicInfos.size());
        Iterator<PartitionInfo> iterator = topicInfos.iterator();
        PartitionInfo partitionInfo = iterator.next();
        Assertions.assertEquals(partitionInfo.getTopicId(), results.get(CorrelationIdConstants.TOPIC_ID));
        Assertions.assertEquals(partitionInfo.getTopic(), topic);
        Set<String> allTopics = support.getAllTopics();
        Assertions.assertNotNull(allTopics);
        Assertions.assertEquals(1, allTopics.size());
        String result = allTopics.iterator().next();
        Assertions.assertNotNull(result);
        Assertions.assertEquals(result, topic);
    }

    /**
     * Tests the retrieval of partition information for a specified topic.
     * <p>
     * This method validates that the partition information obtained from
     * a topic matches the expected results. It first creates a topic with
     * specific partitions and replicas, and then retrieves and checks the
     * partition info to ensure correctness. Assertions are used to confirm
     * that the size of the result map, and the presence and correctness of
     * the topic ID and partition replicas are as expected.
     *
     * @throws Exception if an error occurs during the topic creation or partition information retrieval.
     */
    @Test
    public void testGetPartitionInfo() throws Exception {
        Map<String, Object> results = support.createTopic(topic, partitions, replicas, null);
        Assertions.assertEquals(2, results.size());
        Assertions.assertNotNull(results.get(CorrelationIdConstants.TOPIC_ID));
        Assertions.assertNotNull(results.get(CorrelationIdConstants.PARTITION_REPLICAS));

        TopicPartition topicPartition = new TopicPartition(topic, 0);
        PartitionInfo partitionInfo = support.getPartitionInfo(topicPartition);
        Assertions.assertNotNull(partitionInfo);
        Assertions.assertEquals(partitionInfo.getTopicId(), results.get(CorrelationIdConstants.TOPIC_ID));
        Assertions.assertEquals(partitionInfo.getTopic(), topic);
    }

    /**
     * Tears down the test environment after each test case is executed.
     * This method ensures that resources such as the support and server are properly closed and cleaned up.
     *
     * @throws Exception if any error occurs during the shutdown of support or closing of the server.
     */
    @After
    public void tearDown() throws Exception {
        support.shutdown();
        server.close();
    }
}
