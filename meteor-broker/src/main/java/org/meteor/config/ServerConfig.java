package org.meteor.config;

import java.util.Properties;

public class ServerConfig {
    protected final CommonConfig commonConfig;
    protected final ChunkRecordDispatchConfig chunkRecordDispatchConfig;
    protected final MessageConfig messageConfig;
    protected final MetricsConfig metricsConfig;
    protected final NetworkConfig networkConfig;
    protected final RecordDispatchConfig recordDispatchConfig;
    protected final ZookeeperConfig zookeeperConfig;
    protected final SegmentConfig segmentConfig;

    public ServerConfig(Properties properties) {
        this.commonConfig = new CommonConfig(properties);
        this.chunkRecordDispatchConfig = new ChunkRecordDispatchConfig(properties);
        this.messageConfig = new MessageConfig(properties);
        this.metricsConfig = new MetricsConfig(properties);
        this.networkConfig = new NetworkConfig(properties);
        this.recordDispatchConfig = new RecordDispatchConfig(properties);
        this.zookeeperConfig = new ZookeeperConfig(properties);
        this.segmentConfig = new SegmentConfig(properties);
    }

    public CommonConfig getCommonConfig() {
        return commonConfig;
    }

    public ChunkRecordDispatchConfig getChunkRecordDispatchConfig() {
        return chunkRecordDispatchConfig;
    }

    public MessageConfig getMessageConfig() {
        return messageConfig;
    }

    public MetricsConfig getMetricsConfig() {
        return metricsConfig;
    }

    public NetworkConfig getNetworkConfig() {
        return networkConfig;
    }

    public RecordDispatchConfig getRecordDispatchConfig() {
        return recordDispatchConfig;
    }

    public ZookeeperConfig getZookeeperConfig() {
        return zookeeperConfig;
    }

    public SegmentConfig getSegmentConfig() {
        return segmentConfig;
    }
}
