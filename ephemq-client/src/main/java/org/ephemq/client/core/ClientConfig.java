package org.ephemq.client.core;

import io.netty.util.NettyRuntime;

import java.util.ArrayList;
import java.util.List;

/**
 * Configuration class for client settings.
 * <p>
 * This class encapsulates various configuration properties required to set up and manage a client,
 * including socket preferences, buffer sizes, timeouts, and thread limits. Users can get and set
 * these properties to fine-tune the client behavior.
 */
public class ClientConfig {
    /**
     * A list of addresses used for initial connection to a cluster of servers.
     * This variable holds the bootstrap servers' addresses that the client
     * can use to initiate a connection.
     */
    private List<String> bootstrapAddresses = new ArrayList<>();
    /**
     * Indicates whether to prefer using the epoll transport for socket I/O.
     * The default value is {@code true}.
     */
    private boolean socketEpollPrefer = true;
    /**
     * Specifies the size of the socket send buffer for network communications.
     */
    private int socketSendBufferSize = 65536;
    /**
     * The size of the receive buffer for socket connections in bytes.
     * This buffer temporarily stores data received from the network before it is processed.
     * The default value is set to 65536 bytes.
     */
    private int socketReceiveBufferSize = 65536;
    /**
     * Timeout value in milliseconds for establishing a connection to a channel.
     * This value specifies how long the client should wait before abandoning the attempt
     * to connect to a channel.
     */
    private int channelConnectionTimeoutMilliseconds = 3000;
    /**
     * The period (in milliseconds) to keep an idle channel active before closing it.
     */
    private int channelKeepPeriodMilliseconds = 20000;
    /**
     * The timeout duration in milliseconds for a channel to be considered idle.
     * If no activity is detected on the channel within this time frame, the channel
     * might be closed to conserve system resources.
     */
    private int channelIdleTimeoutMilliseconds = 30000;
    /**
     * Defines the maximum number of permits available for concurrency control when invoking
     * channels in the client configuration. This value controls the number of concurrent
     * channel invocations that can be made, helping to manage load and resource usage.
     */
    private int channelInvokePermits = 100;
    /**
     * Specifies the upper limit for the number of worker threads that can be created.
     * It initializes to the number of available processors on the host system.
     */
    private int workerThreadLimit = NettyRuntime.availableProcessors();
    /**
     * The timeout duration in milliseconds for fetching metadata.
     * This value determines how long the client will wait for
     * metadata retrieval operations to complete before timing out.
     */
    private int metadataTimeoutMilliseconds = 5000;
    /**
     * The period in milliseconds that the client will wait before refreshing
     * metadata information. This configuration is useful for optimizing
     * performance by controlling how often the client updates its metadata
     * state.
     */
    private int metadataRefreshPeriodMilliseconds = 5000;
    /**
     * The maximum number of connections that can be pooled and reused by the client.
     */
    private int connectionPoolCapacity = 1;
    /**
     * Timeout duration in milliseconds for the operation that creates a new topic.
     */
    private int createTopicTimeoutMilliseconds = 2000;
    /**
     * Timeout duration in milliseconds for deleting a topic.
     * This value determines how long the client will wait for a response
     * from the server before timing out an attempt to delete a topic.
     */
    private int deleteTopicTimeoutMilliseconds = 2000;
    /**
     * The timeout duration in milliseconds for calculating the number
     * of partitions in a client's operation. This value determines how
     * long the client should wait for the partition information before
     * timing out.
     */
    private int calculatePartitionsTimeoutMilliseconds = 5000;
    /**
     * The timeout duration in milliseconds for ledger migration operations.
     * If the migration process takes longer than this value, it will be terminated.
     * This setting ensures operations complete in a timely manner, preventing indefinite waits.
     */
    private int migrateLedgerTimeoutMilliseconds = 5000;
    private boolean isPreferIoUring = false;

    public boolean isPreferIoUring() {
        return isPreferIoUring;
    }

    public void setPreferIoUring(boolean perferIoUring) {
        this.isPreferIoUring = perferIoUring;
    }

    /**
     * Retrieves the timeout duration, in milliseconds, for migrating a ledger.
     *
     * @return the migration timeout in milliseconds.
     */
    public int getMigrateLedgerTimeoutMilliseconds() {
        return migrateLedgerTimeoutMilliseconds;
    }

    /**
     * Sets the timeout for migrating the ledger in milliseconds.
     *
     * @param migrateLedgerTimeoutMilliseconds the timeout value for migrating the ledger in milliseconds
     */
    public void setMigrateLedgerTimeoutMilliseconds(int migrateLedgerTimeoutMilliseconds) {
        this.migrateLedgerTimeoutMilliseconds = migrateLedgerTimeoutMilliseconds;
    }

    /**
     * Gets the timeout value in milliseconds for calculating partitions.
     *
     * @return the timeout value for calculating partitions in milliseconds.
     */
    public int getCalculatePartitionsTimeoutMilliseconds() {
        return calculatePartitionsTimeoutMilliseconds;
    }

    /**
     * Sets the timeout duration for calculating partitions in milliseconds.
     *
     * @param calculatePartitionsTimeoutMilliseconds the timeout in milliseconds for calculating partitions
     */
    public void setCalculatePartitionsTimeoutMilliseconds(int calculatePartitionsTimeoutMilliseconds) {
        this.calculatePartitionsTimeoutMilliseconds = calculatePartitionsTimeoutMilliseconds;
    }

    /**
     * Retrieves the list of bootstrap addresses configured for the client.
     *
     * @return a list of bootstrap addresses
     */
    public List<String> getBootstrapAddresses() {
        return bootstrapAddresses;
    }

    /**
     * Sets the bootstrap addresses for the client configuration.
     *
     * @param bootstrapAddresses A list of bootstrap server addresses.
     */
    public void setBootstrapAddresses(List<String> bootstrapAddresses) {
        this.bootstrapAddresses = bootstrapAddresses;
    }

    /**
     * Checks if the socket epoll is preferred.
     *
     * @return true if socket epoll is preferred, false otherwise.
     */
    public boolean isSocketEpollPrefer() {
        return socketEpollPrefer;
    }

    /**
     * Sets the preference for using the epoll edge-triggered I/O event notification mechanism
     * for socket operations.
     *
     * @param socketEpollPrefer true to prefer using epoll; false otherwise
     */
    public void setSocketEpollPrefer(boolean socketEpollPrefer) {
        this.socketEpollPrefer = socketEpollPrefer;
    }

    /**
     * Retrieves the size of the socket send buffer.
     *
     * @return the size of the socket send buffer in bytes
     */
    public int getSocketSendBufferSize() {
        return socketSendBufferSize;
    }

    /**
     * Sets the socket send buffer size.
     *
     * @param socketSendBufferSize the size of the socket send buffer in bytes
     */
    public void setSocketSendBufferSize(int socketSendBufferSize) {
        this.socketSendBufferSize = socketSendBufferSize;
    }

    /**
     * Retrieves the configured size of the socket receive buffer.
     *
     * @return the size of the socket receive buffer in bytes.
     */
    public int getSocketReceiveBufferSize() {
        return socketReceiveBufferSize;
    }

    /**
     * Sets the size of the socket receive buffer.
     *
     * @param socketReceiveBufferSize the size of the socket receive buffer in bytes
     */
    public void setSocketReceiveBufferSize(int socketReceiveBufferSize) {
        this.socketReceiveBufferSize = socketReceiveBufferSize;
    }

    /**
     * Returns the channel connection timeout value in milliseconds.
     *
     * @return the channel connection timeout value in milliseconds.
     */
    public int getChannelConnectionTimeoutMilliseconds() {
        return channelConnectionTimeoutMilliseconds;
    }

    /**
     * Sets the timeout for channel connection in milliseconds.
     *
     * @param channelConnectionTimeoutMilliseconds the timeout value in milliseconds to set for channel connection
     */
    public void setChannelConnectionTimeoutMilliseconds(int channelConnectionTimeoutMilliseconds) {
        this.channelConnectionTimeoutMilliseconds = channelConnectionTimeoutMilliseconds;
    }

    /**
     * Retrieves the period in milliseconds after which a channel is kept alive.
     *
     * @return the channel keep-period in milliseconds.
     */
    public int getChannelKeepPeriodMilliseconds() {
        return channelKeepPeriodMilliseconds;
    }

    /**
     * Sets the duration in milliseconds for which a channel should be kept open.
     *
     * @param channelKeepPeriodMilliseconds the duration in milliseconds to keep the channel open
     */
    public void setChannelKeepPeriodMilliseconds(int channelKeepPeriodMilliseconds) {
        this.channelKeepPeriodMilliseconds = channelKeepPeriodMilliseconds;
    }

    /**
     * Retrieves the timeout duration in milliseconds for a channel when it is idle.
     *
     * @return the channel idle timeout in milliseconds.
     */
    public int getChannelIdleTimeoutMilliseconds() {
        return channelIdleTimeoutMilliseconds;
    }

    /**
     * Sets the idle timeout duration for the channel in milliseconds.
     *
     * @param channelIdleTimeoutMilliseconds the timeout duration in milliseconds for which the channel is idle
     */
    public void setChannelIdleTimeoutMilliseconds(int channelIdleTimeoutMilliseconds) {
        this.channelIdleTimeoutMilliseconds = channelIdleTimeoutMilliseconds;
    }

    /**
     * Retrieves the number of permits available for channel invocation.
     *
     * @return the number of channel invocation permits as an integer.
     */
    public int getChannelInvokePermits() {
        return channelInvokePermits;
    }

    /**
     * Sets the maximum number of concurrent channel invocations allowed.
     *
     * @param channelInvokePermits the maximum number of concurrent invocations
     */
    public void setChannelInvokePermits(int channelInvokePermits) {
        this.channelInvokePermits = channelInvokePermits;
    }

    /**
     * Retrieves the configured limit for worker threads.
     *
     * @return the maximum number of worker threads allowed.
     */
    public int getWorkerThreadLimit() {
        return workerThreadLimit;
    }

    /**
     * Sets the limit for the worker threads.
     *
     * @param workerThreadLimit the maximum number of worker threads that can be allocated
     */
    public void setWorkerThreadLimit(int workerThreadLimit) {
        this.workerThreadLimit = workerThreadLimit;
    }

    /**
     * Retrieves the timeout value for metadata operations in milliseconds.
     *
     * @return the metadata timeout in milliseconds
     */
    public int getMetadataTimeoutMilliseconds() {
        return metadataTimeoutMilliseconds;
    }

    /**
     * Sets the metadata timeout in milliseconds.
     *
     * @param metadataTimeoutMilliseconds the timeout duration to be set for metadata operations, in milliseconds
     */
    public void setMetadataTimeoutMilliseconds(int metadataTimeoutMilliseconds) {
        this.metadataTimeoutMilliseconds = metadataTimeoutMilliseconds;
    }

    /**
     * Gets the period, in milliseconds, at which the metadata is refreshed.
     *
     * @return the metadata refresh period in milliseconds
     */
    public int getMetadataRefreshPeriodMilliseconds() {
        return metadataRefreshPeriodMilliseconds;
    }

    /**
     * Sets the period in milliseconds for refreshing metadata.
     *
     * @param metadataRefreshPeriodMilliseconds the period in milliseconds to refresh metadata
     */
    public void setMetadataRefreshPeriodMilliseconds(int metadataRefreshPeriodMilliseconds) {
        this.metadataRefreshPeriodMilliseconds = metadataRefreshPeriodMilliseconds;
    }

    /**
     * Retrieves the configured capacity of the connection pool.
     *
     * @return the maximum number of connections that can be held in the connection pool.
     */
    public int getConnectionPoolCapacity() {
        return connectionPoolCapacity;
    }

    /**
     * Sets the capacity of the connection pool.
     *
     * @param connectionPoolCapacity the maximum number of connections that the pool can hold
     */
    public void setConnectionPoolCapacity(int connectionPoolCapacity) {
        this.connectionPoolCapacity = connectionPoolCapacity;
    }

    /**
     * Retrieves the timeout value for creating a new topic, in milliseconds.
     *
     * @return the timeout value in milliseconds for the create topic operation.
     */
    public int getCreateTopicTimeoutMilliseconds() {
        return createTopicTimeoutMilliseconds;
    }

    /**
     * Sets the timeout duration for the topic creation process in milliseconds.
     *
     * @param createTopicTimeoutMilliseconds the timeout duration to set, in milliseconds
     */
    public void setCreateTopicTimeoutMilliseconds(int createTopicTimeoutMilliseconds) {
        this.createTopicTimeoutMilliseconds = createTopicTimeoutMilliseconds;
    }

    /**
     * Gets the timeout period, in milliseconds, for deleting a topic.
     *
     * @return the timeout period for deleting a topic in milliseconds
     */
    public int getDeleteTopicTimeoutMilliseconds() {
        return deleteTopicTimeoutMilliseconds;
    }

    /**
     * Sets the timeout in milliseconds for deleting a topic.
     *
     * @param deleteTopicTimeoutMilliseconds the timeout duration in milliseconds
     */
    public void setDeleteTopicTimeoutMilliseconds(int deleteTopicTimeoutMilliseconds) {
        this.deleteTopicTimeoutMilliseconds = deleteTopicTimeoutMilliseconds;
    }
}
