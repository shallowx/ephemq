package org.meteor.configuration;

import java.util.Properties;

public class ServerConfiguration {
    protected final CommonConfiguration commonConfiguration;
    protected final ChunkRecordDispatchConfiguration chunkRecordDispatchConfiguration;
    protected final MessageConfiguration messageConfiguration;
    protected final MetricsConfiguration metricsConfiguration;
    protected final NetworkConfiguration networkConfiguration;
    protected final RecordDispatchConfiguration recordDispatchConfiguration;
    protected final ZookeeperConfiguration zookeeperConfiguration;
    protected final SegmentConfiguration segmentConfiguration;

    public ServerConfiguration (Properties properties) {
        this.commonConfiguration = new CommonConfiguration(properties);
        this.chunkRecordDispatchConfiguration = new ChunkRecordDispatchConfiguration(properties);
        this.messageConfiguration = new MessageConfiguration(properties);
        this.metricsConfiguration = new MetricsConfiguration(properties);
        this.networkConfiguration = new NetworkConfiguration(properties);
        this.recordDispatchConfiguration = new RecordDispatchConfiguration(properties);
        this.zookeeperConfiguration = new ZookeeperConfiguration(properties);
        this.segmentConfiguration =new SegmentConfiguration(properties);
    }

    public CommonConfiguration getCommonConfiguration() {
        return commonConfiguration;
    }

    public ChunkRecordDispatchConfiguration getChunkRecordDispatchConfiguration() {
        return chunkRecordDispatchConfiguration;
    }

    public MessageConfiguration getMessageConfiguration() {
        return messageConfiguration;
    }

    public MetricsConfiguration getMetricsConfiguration() {
        return metricsConfiguration;
    }

    public NetworkConfiguration getNetworkConfiguration() {
        return networkConfiguration;
    }

    public RecordDispatchConfiguration getRecordDispatchConfiguration() {
        return recordDispatchConfiguration;
    }

    public ZookeeperConfiguration getZookeeperConfiguration() {
        return zookeeperConfiguration;
    }

    public SegmentConfiguration getSegmentConfiguration() {
        return segmentConfiguration;
    }
}
