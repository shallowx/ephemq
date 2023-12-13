package org.meteor.configuration;

import java.util.Properties;

public class ServerConfiguration {
    private final CommonConfiguration commonConfiguration;
    private final ChunkRecordDispatchConfiguration chunkRecordDispatchConfiguration;
    private final MessageConfiguration messageConfiguration;
    private final MetricsConfiguration metricsConfiguration;
    private final NetworkConfiguration networkConfiguration;
    private final RecordDispatchConfiguration recordDispatchConfiguration;
    private final ZookeeperConfiguration zookeeperConfiguration;
    private final ProxyConfiguration proxyConfiguration;
    private final SegmentConfiguration segmentConfiguration;

    public ServerConfiguration (Properties properties) {
        this.commonConfiguration = new CommonConfiguration(properties);
        this.chunkRecordDispatchConfiguration = new ChunkRecordDispatchConfiguration(properties);
        this.messageConfiguration = new MessageConfiguration(properties);
        this.metricsConfiguration = new MetricsConfiguration(properties);
        this.networkConfiguration = new NetworkConfiguration(properties);
        this.recordDispatchConfiguration = new RecordDispatchConfiguration(properties);
        this.zookeeperConfiguration = new ZookeeperConfiguration(properties);
        this.segmentConfiguration =new SegmentConfiguration(properties);
        this.proxyConfiguration =new ProxyConfiguration(properties, this.commonConfiguration, this.networkConfiguration, this.zookeeperConfiguration, this
                .segmentConfiguration);
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

    public ProxyConfiguration getProxyConfiguration() {
        return proxyConfiguration;
    }

    public SegmentConfiguration getSegmentConfiguration() {
        return segmentConfiguration;
    }
}
