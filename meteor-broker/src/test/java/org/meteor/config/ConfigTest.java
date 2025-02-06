package org.meteor.config;

import io.netty.util.NettyRuntime;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.Properties;

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
        Assertions.assertEquals("test-server-id", commonConfig.getServerId());
        Assertions.assertNotEquals("default", commonConfig.getServerId());

        MessageConfig messageConfig = config.getMessageConfig();
        Assertions.assertNotNull(messageConfig);
        Assertions.assertNotEquals(messageConfig.getMessageSyncThreadLimit(), NettyRuntime.availableProcessors());
        Assertions.assertEquals(100, messageConfig.getMessageSyncThreadLimit());

        MetricsConfig metricsConfig = config.getMetricsConfig();
        Assertions.assertNotNull(metricsConfig);
        Assertions.assertNotEquals(100, metricsConfig.getMetricsSampleLimit());
        Assertions.assertEquals(10, metricsConfig.getMetricsSampleLimit());

        NetworkConfig networkConfig = config.getNetworkConfig();
        Assertions.assertNotNull(networkConfig);
        Assertions.assertNotEquals(30000, networkConfig.getConnectionTimeoutMilliseconds());
        Assertions.assertEquals(100, networkConfig.getConnectionTimeoutMilliseconds());

        ChunkDispatchConfig chunkRecordDispatchConfig = config.getChunkRecordDispatchConfig();
        Assertions.assertNotNull(chunkRecordDispatchConfig);
        Assertions.assertNotEquals(50, chunkRecordDispatchConfig.getChunkDispatchEntryLoadLimit());
        Assertions.assertEquals(100, chunkRecordDispatchConfig.getChunkDispatchEntryLoadLimit());

        DefaultDispatchConfig recordDispatchConfig = config.getRecordDispatchConfig();
        Assertions.assertNotNull(recordDispatchConfig);
        Assertions.assertNotEquals(50, recordDispatchConfig.getDispatchEntryLoadLimit());
        Assertions.assertEquals(100, recordDispatchConfig.getDispatchEntryLoadLimit());

        SegmentConfig segmentConfig = config.getSegmentConfig();
        Assertions.assertNotNull(segmentConfig);
        Assertions.assertNotEquals(4194304, segmentConfig.getSegmentRollingSize());
        Assertions.assertEquals(100, segmentConfig.getSegmentRollingSize());

        ZookeeperConfig zookeeperConfig = config.getZookeeperConfig();
        Assertions.assertNotNull(zookeeperConfig);
        Assertions.assertNotEquals("localhost:2181", zookeeperConfig.getZookeeperUrl());
        Assertions.assertEquals("0.0.0.0:9527", zookeeperConfig.getZookeeperUrl());
    }
}
