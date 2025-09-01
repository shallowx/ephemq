package org.ephemq.config;

import io.netty.util.NettyRuntime;

import java.util.Properties;

import static org.ephemq.common.util.ObjectLiteralsTransformUtil.object2Boolean;
import static org.ephemq.common.util.ObjectLiteralsTransformUtil.object2Int;

/**
 * The NetworkConfig class manages network-related configuration properties.
 * It retrieves config values from a provided Properties instance and provides
 * utility methods to access these values with appropriate defaults.
 */
public class NetworkConfig {
    /**
     * Configuration property key for the network connection timeout duration in milliseconds.
     * This value determines how long the system will wait for a network connection to be established
     * before timing out.
     */
    private static final String CONNECTION_TIMEOUT_MILLISECONDS = "connection.timeout.milliseconds";
    /**
     * Configuration property key to enable or disable network log debug mode.
     * Used to retrieve the value indicating whether debug logging for networking
     * should be enabled or not.
     */
    private static final String NETWORK_LOG_DEBUG_ENABLED = "network.log.debug.enabled";
    /**
     * Property key for the high watermark configuration of the socket write buffer.
     * Used to specify the high watermark value, which determines the maximum
     * allowable write buffer size before backpressure is applied.
     */
    private static final String WRITE_BUFFER_WATER_MARK = "socket.write.buffer.high.watermark";
    /**
     * The property key for limiting the number of network threads.
     * This value is used to configure the maximum number of threads
     * that can be used for network operations.
     */
    private static final String NETWORK_THREAD_LIMIT = "network.thread.limit";
    /**
     * Configuration key for specifying the limit on the number of I/O threads.
     * This property is used to control the maximum number of threads dedicated
     * to I/O operations in the network configuration.
     */
    private static final String IO_THREAD_LIMIT = "io.thread.limit";
    /**
     * Configuration key representing the timeout duration in milliseconds for notifying clients.
     * This value is used to specify how long the system should wait before timing out an
     * attempt to notify a client.
     */
    private static final String NOTIFY_CLIENT_TIMEOUT_MILLISECONDS = "notify.client.timeout.milliseconds";
    /**
     * A Properties object that holds network configuration values.
     * This variable is utilized by the NetworkConfig class to retrieve
     * various network-related settings such as connection timeouts,
     * buffer sizes, and thread limits.
     */
    private final Properties prop;

    /**
     * Initializes a new instance of the NetworkConfig class with the specified properties.
     *
     * @param prop the Properties object containing the network configuration settings.
     */
    public NetworkConfig(Properties prop) {
        this.prop = prop;
    }

    /**
     * Checks if network log debugging is enabled based on the configuration properties.
     *
     * @return true if network log debugging is enabled, false otherwise
     */
    public boolean isNetworkLogDebugEnabled() {
        return object2Boolean(prop.getOrDefault(NETWORK_LOG_DEBUG_ENABLED, false));
    }

    /**
     * Gets the write buffer water mark value from the properties.
     * This value determines the high water mark for the write buffer,
     * which is the point where the buffer is considered full and no further writes can occur
     * until the buffer is cleared to below the low water mark.
     *
     * @return the write buffer water mark in bytes, defaulting to 30 MB (30 * 1024 * 1024) if not specified.
     */
    public int getWriteBufferWaterMarker() {
        return object2Int(prop.getOrDefault(WRITE_BUFFER_WATER_MARK, 30 * 1024 * 1024));
    }

    /**
     * Retrieves the connection timeout duration in milliseconds.
     * If the property is not explicitly set, a default value of 30000 milliseconds is returned.
     *
     * @return the connection timeout in milliseconds
     */
    public int getConnectionTimeoutMilliseconds() {
        return object2Int(prop.getOrDefault(CONNECTION_TIMEOUT_MILLISECONDS, 30000));
    }

    /**
     * Retrieves the configured network thread limit.
     * If the property is not explicitly set, it defaults to the number of available processors multiplied by 4.
     *
     * @return the network thread limit
     */
    public int getNetworkThreadLimit() {
        return object2Int(prop.getOrDefault(NETWORK_THREAD_LIMIT, NettyRuntime.availableProcessors() * 4));
    }

    /**
     * Retrieves the IO thread limit from the network configuration properties.
     * If the IO thread limit property is not set, it returns the default value of 1.
     *
     * @return the configured IO thread limit, or the default value of 1 if not set
     */
    public int getIoThreadLimit() {
        return object2Int(prop.getOrDefault(IO_THREAD_LIMIT, 1));
    }

    /**
     * Retrieves the timeout duration in milliseconds for notifying clients.
     * If the "notify.client.timeout.milliseconds" property is not set, it defaults to 100 milliseconds.
     *
     * @return the timeout duration in milliseconds for notifying clients.
     */
    public int getNotifyClientTimeoutMilliseconds() {
        return object2Int(prop.getOrDefault(NOTIFY_CLIENT_TIMEOUT_MILLISECONDS, 100));
    }
}
