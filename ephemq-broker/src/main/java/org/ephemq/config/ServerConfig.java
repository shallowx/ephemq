package org.ephemq.config;

import java.util.Properties;

/**
 * The ServerConfig class is responsible for initializing and providing access to various server configurations.
 * It aggregates multiple configuration objects and initializes them using properties specified during construction.
 */
public class ServerConfig {
    /**
     * Provides a unified configuration management for server settings.
     * <p>
     * This variable holds an instance of CommonConfig, which initializes various
     * server configuration parameters from a Properties object. It provides methods
     * to retrieve configuration values in appropriate types.
     */
    protected final CommonConfig commonConfig;
    /**
     * Configuration for chunk record dispatch settings aggregated in the ServerConfig.
     * This field initializes and provides access to properties related to chunk
     * dispatch within the server.
     */
    protected final ChunkDispatchConfig chunkRecordDispatchConfig;
    /**
     * Represents the configuration settings specifically related to message handling.
     * This instance is initialized within the ServerConfig class and provides access
     * to properties that control message sync, storage, and dispatch thread limits.
     */
    protected final MessageConfig messageConfig;
    /**
     * Represents the configuration settings for metrics in the server.
     * This object is initialized and provided by the ServerConfig class and
     * contains various metrics-specific configurations derived from the provided properties.
     */
    protected final MetricsConfig metricsConfig;
    /**
     * Holds the network-related configuration settings for the server.
     * This involves properties such as connection timeouts, buffer sizes, and thread limits
     * that are crucial for network operations.
     */
    protected final NetworkConfig networkConfig;
    /**
     * Configuration settings for handling record dispatch operations.
     * This configuration object includes various limits and timeout settings
     * for dispatch operations, such as load limits, follow limits, pursue limits,
     * alignment limits, and pursue timeouts.
     * These settings are initialized based on the properties provided.
     */
    protected final DefaultDispatchConfig recordDispatchConfig;
    /**
     * Holds the configuration properties for connecting to a Zookeeper instance.
     * <p>
     * This configuration object contains properties such as the Zookeeper URL,
     * connection retry sleep time, number of connection retries, connection timeout,
     * and session timeout, which are essential for establishing and maintaining a connection
     * to the Zookeeper service.
     */
    protected final ZookeeperConfig zookeeperConfig;
    /**
     * SegmentConfig manages the configuration settings related to segments.
     * It allows retrieval of segment rolling size, segment retain limit,
     * and segment retention time in milliseconds from a provided Properties object.
     */
    protected final SegmentConfig segmentConfig;

    /**
     * Constructs a ServerConfig instance and initializes various server configuration parameters
     * using the provided properties.
     *
     * @param properties The properties used to initialize server configurations.
     */
    public ServerConfig(Properties properties) {
        this.commonConfig = new CommonConfig(properties);
        this.chunkRecordDispatchConfig = new ChunkDispatchConfig(properties);
        this.messageConfig = new MessageConfig(properties);
        this.metricsConfig = new MetricsConfig(properties);
        this.networkConfig = new NetworkConfig(properties);
        this.recordDispatchConfig = new DefaultDispatchConfig(properties);
        this.zookeeperConfig = new ZookeeperConfig(properties);
        this.segmentConfig = new SegmentConfig(properties);
    }

    /**
     * Retrieves the CommonConfig instance that contains configuration parameters
     * shared across the server.
     *
     * @return the CommonConfig instance containing common server configurations
     */
    public CommonConfig getCommonConfig() {
        return commonConfig;
    }

    /**
     * Provides access to the chunk record dispatch configuration.
     *
     * @return the ChunkDispatchConfig object containing settings for chunk record dispatch.
     */
    public ChunkDispatchConfig getChunkRecordDispatchConfig() {
        return chunkRecordDispatchConfig;
    }

    /**
     * Retrieves the MessageConfig object, which contains configuration properties
     * related to message handling such as synchronization, storage, and dispatch thread limits.
     *
     * @return the MessageConfig object containing message handling configuration properties
     */
    public MessageConfig getMessageConfig() {
        return messageConfig;
    }

    /**
     * Retrieves the metrics configuration.
     *
     * @return the MetricsConfig object that encapsulates metrics-related configuration settings
     */
    public MetricsConfig getMetricsConfig() {
        return metricsConfig;
    }

    /**
     * Gets the NetworkConfig instance from the ServerConfig.
     *
     * @return the NetworkConfig instance that holds network-related configuration properties.
     */
    public NetworkConfig getNetworkConfig() {
        return networkConfig;
    }

    /**
     * Retrieves the configuration settings for dispatch operations.
     *
     * @return an instance of DefaultDispatchConfig containing the dispatch configurations.
     */
    public DefaultDispatchConfig getRecordDispatchConfig() {
        return recordDispatchConfig;
    }

    /**
     * Retrieves the current Zookeeper configuration.
     *
     * @return the ZookeeperConfig object containing Zookeeper configuration settings.
     */
    public ZookeeperConfig getZookeeperConfig() {
        return zookeeperConfig;
    }

    /**
     * Retrieves the configuration settings related to segments.
     *
     * @return the SegmentConfig object containing the segment-related configurations.
     */
    public SegmentConfig getSegmentConfig() {
        return segmentConfig;
    }
}
