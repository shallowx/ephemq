package org.meteor.configuration;

import java.util.Properties;

public class ServerConfig {
    protected final CommonConfig commonConfiguration;
    protected final ChunkRecordDispatchConfig chunkRecordDispatchConfiguration;
    protected final MessageConfig messageConfiguration;
    protected final MetricsConfig metricsConfiguration;
    protected final NetworkConfig networkConfiguration;
    protected final RecordDispatchConfig recordDispatchConfiguration;
    protected final ZookeeperConfig zookeeperConfiguration;
    protected final SegmentConfig segmentConfiguration;

    public ServerConfig(Properties properties) {
        this.commonConfiguration = new CommonConfig(properties);
        this.chunkRecordDispatchConfiguration = new ChunkRecordDispatchConfig(properties);
        this.messageConfiguration = new MessageConfig(properties);
        this.metricsConfiguration = new MetricsConfig(properties);
        this.networkConfiguration = new NetworkConfig(properties);
        this.recordDispatchConfiguration = new RecordDispatchConfig(properties);
        this.zookeeperConfiguration = new ZookeeperConfig(properties);
        this.segmentConfiguration =new SegmentConfig(properties);
    }

    public CommonConfig getCommonConfiguration() {
        return commonConfiguration;
    }

    public ChunkRecordDispatchConfig getChunkRecordDispatchConfiguration() {
        return chunkRecordDispatchConfiguration;
    }

    public MessageConfig getMessageConfiguration() {
        return messageConfiguration;
    }

    public MetricsConfig getMetricsConfiguration() {
        return metricsConfiguration;
    }

    public NetworkConfig getNetworkConfiguration() {
        return networkConfiguration;
    }

    public RecordDispatchConfig getRecordDispatchConfiguration() {
        return recordDispatchConfiguration;
    }

    public ZookeeperConfig getZookeeperConfiguration() {
        return zookeeperConfiguration;
    }

    public SegmentConfig getSegmentConfiguration() {
        return segmentConfiguration;
    }
}
