package org.meteor.config;

import io.netty.util.NettyRuntime;
import java.util.Properties;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class ConfigTest {
    /**
     * Tests the configuration settings loaded into a ServerConfig instance.
     * <p>
     * This method initializes a Properties object with specific configuration
     * keys and values, then creates a ServerConfig instance using these properties.
     * Assertions are used to verify that various configuration settings
     * within the ServerConfig object are correctly initialized and
     * deviate from their potential default values.
     * <p>
     * Key configuration aspects tested include:
     * - CommonConfig for general server settings.
     * - MessageConfig for message synchronization limits.
     * - MetricsConfig for metrics sampling limits.
     * - NetworkConfig for connection timeout settings.
     * - ChunkDispatchConfig for chunk dispatch entry limits.
     * - DefaultDispatchConfig for general dispatch entry limits.
     * - SegmentConfig for segment rolling sizes.
     * - ZookeeperConfig for Zookeeper connection URL.
     */
    @Test
    public void testConfig() {
        Properties properties = new Properties();
        properties.put("server.id", "test-server-id");
        properties.put("message.sync.thread.limit", 100);
        properties.put("metrics.sample.limit", 10);
        properties.put("connection.timeout.milliseconds", 100);
        properties.put("chunk.dispatch.entry.load.limit", 100);
        properties.put("dispatch.entry.load.limit", 100);
        properties.put("segment.rolling.size", 100);
        properties.put("zookeeper.url", "0.0.0.0:9527");

        ServerConfig config = new ServerConfig(properties);
        CommonConfig commonConfig = config.getCommonConfig();
        Assertions.assertNotNull(commonConfig);
        Assertions.assertEquals(commonConfig.getServerId(), "test-server-id");
        Assertions.assertNotEquals(commonConfig.getServerId(), "default");

        MessageConfig messageConfig = config.getMessageConfig();
        Assertions.assertNotNull(messageConfig);
        Assertions.assertNotEquals(messageConfig.getMessageSyncThreadLimit(), NettyRuntime.availableProcessors());
        Assertions.assertEquals(messageConfig.getMessageSyncThreadLimit(), 100);

        MetricsConfig metricsConfig = config.getMetricsConfig();
        Assertions.assertNotNull(metricsConfig);
        Assertions.assertNotEquals(metricsConfig.getMetricsSampleLimit(), 100);
        Assertions.assertEquals(metricsConfig.getMetricsSampleLimit(), 10);

        NetworkConfig networkConfig = config.getNetworkConfig();
        Assertions.assertNotNull(networkConfig);
        Assertions.assertNotEquals(networkConfig.getConnectionTimeoutMilliseconds(), 30000);
        Assertions.assertEquals(networkConfig.getConnectionTimeoutMilliseconds(), 100);

        ChunkDispatchConfig chunkRecordDispatchConfig = config.getChunkRecordDispatchConfig();
        Assertions.assertNotNull(chunkRecordDispatchConfig);
        Assertions.assertNotEquals(chunkRecordDispatchConfig.getChunkDispatchEntryLoadLimit(), 50);
        Assertions.assertEquals(chunkRecordDispatchConfig.getChunkDispatchEntryLoadLimit(), 100);

        DefaultDispatchConfig recordDispatchConfig = config.getRecordDispatchConfig();
        Assertions.assertNotNull(recordDispatchConfig);
        Assertions.assertNotEquals(recordDispatchConfig.getDispatchEntryLoadLimit(), 50);
        Assertions.assertEquals(recordDispatchConfig.getDispatchEntryLoadLimit(), 100);

        SegmentConfig segmentConfig = config.getSegmentConfig();
        Assertions.assertNotNull(segmentConfig);
        Assertions.assertNotEquals(segmentConfig.getSegmentRollingSize(), 4194304);
        Assertions.assertEquals(segmentConfig.getSegmentRollingSize(), 100);

        ZookeeperConfig zookeeperConfig = config.getZookeeperConfig();
        Assertions.assertNotNull(zookeeperConfig);
        Assertions.assertNotEquals(zookeeperConfig.getZookeeperUrl(), "localhost:2181");
        Assertions.assertEquals(zookeeperConfig.getZookeeperUrl(), "0.0.0.0:9527");
    }
}
