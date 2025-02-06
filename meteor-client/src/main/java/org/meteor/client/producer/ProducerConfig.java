package org.meteor.client.producer;

import org.meteor.client.core.ClientConfig;
import org.meteor.common.compression.CompressionType;

/**
 * Configuration class for a message producer.
 * <p>
 * This class encapsulates various configuration settings
 * that influence the behavior of a message producer in a messaging system.
 */
public class ProducerConfig {
    /**
     * Holds the configuration settings related to the client.
     * <p>
     * This includes all necessary parameters required to configure and manage
     * the client's behavior in the messaging system.
     */
    private ClientConfig clientConfig = new ClientConfig();
    /**
     * Specifies the timeout duration in milliseconds for sending messages.
     * <p>
     * This variable determines the maximum time the system will wait
     * for a send operation to complete before it times out.
     * A value of 2000 milliseconds (2 seconds) is set as the default timeout duration.
     */
    private int sendTimeoutMilliseconds = 2000;
    /**
     * The timeout value in milliseconds for sending messages asynchronously.
     * <p>
     * This variable determines the maximum amount of time the system will wait for an
     * asynchronous message send operation to complete before timing out.
     */
    private int sendAsyncTimeoutMilliseconds = 2000;
    /**
     * Timeout duration in milliseconds for one-way send operations.
     * <p>
     * This value specifies the maximum time the system will wait
     * for the completion of a one-way send operation before timing out.
     */
    private int sendOnewayTimeoutMilliseconds = 2000;
    /**
     * The level of compression to apply to the message payload.
     * <p>
     * This value influences the trade-off between the speed of compression and
     * the size reduction of the message payload. Higher values typically provide
     * better compression but require more processing time, whereas lower values
     * are faster but result in less compression.
     * <p>
     * Default value is 5.
     */
    private int compressLevel = 5;
    /**
     * Specifies the type of compression to be used for message data.
     * <p>
     * This setting determines the algorithm applied to compress and decompress
     * message contents during transmission to optimize bandwidth and storage utilization.
     * The chosen algorithm should balance the trade-offs between compression efficiency
     * and processing speed according to the requirements of the messaging system.
     */
    private CompressionType compressionType;
    /**
     * Indicates whether message compression is enabled in the producer configuration.
     * If true, messages will be compressed before being sent.
     */
    private boolean enableCompression;

    /**
     * Checks if compression is enabled for the message producer.
     *
     * @return true if compression is enabled, false otherwise.
     */
    public boolean isEnableCompression() {
        return enableCompression;
    }

    /**
     * Enables or disables message compression.
     *
     * @param enableCompression true to enable compression, false to disable it.
     */
    public void setEnableCompression(boolean enableCompression) {
        this.enableCompression = enableCompression;
    }

    /**
     * Retrieves the client configuration for the message producer.
     *
     * @return the client configuration of this message producer
     */
    public ClientConfig getClientConfig() {
        return clientConfig;
    }

    /**
     * Sets the configuration for the client.
     *
     * @param clientConfig the configuration settings for the client
     */
    public void setClientConfig(ClientConfig clientConfig) {
        this.clientConfig = clientConfig;
    }

    /**
     * Retrieves the timeout duration in milliseconds for sending a message.
     *
     * @return the send timeout in milliseconds
     */
    public int getSendTimeoutMilliseconds() {
        return sendTimeoutMilliseconds;
    }

    /**
     * Sets the timeout duration in milliseconds for sending messages.
     *
     * @param sendTimeoutMilliseconds the timeout duration in milliseconds
     */
    public void setSendTimeoutMilliseconds(int sendTimeoutMilliseconds) {
        this.sendTimeoutMilliseconds = sendTimeoutMilliseconds;
    }

    /**
     * Returns the timeout duration for sending messages asynchronously.
     *
     * @return the send asynchronous timeout in milliseconds
     */
    public int getSendAsyncTimeoutMilliseconds() {
        return sendAsyncTimeoutMilliseconds;
    }

    /**
     * Sets the timeout duration for asynchronous send operations in milliseconds.
     *
     * @param sendAsyncTimeoutMilliseconds the timeout duration in milliseconds for asynchronous send operations
     */
    public void setSendAsyncTimeoutMilliseconds(int sendAsyncTimeoutMilliseconds) {
        this.sendAsyncTimeoutMilliseconds = sendAsyncTimeoutMilliseconds;
    }

    /**
     * Gets the timeout value in milliseconds for sending a message in a one-way manner.
     *
     * @return the timeout value in milliseconds for one-way message sending.
     */
    public int getSendOnewayTimeoutMilliseconds() {
        return sendOnewayTimeoutMilliseconds;
    }

    /**
     * Sets the timeout duration for oneway send operations in milliseconds.
     * <p>
     * Oneway send operations are fire-and-forget operations where the sender
     * does not wait for any acknowledgment from the receiver. This timeout value
     * specifies the maximum duration the sender should wait before considering
     * the operation as timed out.
     *
     * @param sendOnewayTimeoutMilliseconds the timeout duration in milliseconds
     */
    public void setSendOnewayTimeoutMilliseconds(int sendOnewayTimeoutMilliseconds) {
        this.sendOnewayTimeoutMilliseconds = sendOnewayTimeoutMilliseconds;
    }

    /**
     * Gets the compression level for the message producer.
     *
     * @return the compression level as an integer. A higher value typically indicates a higher level of compression.
     */
    public int getCompressLevel() {
        return compressLevel;
    }

    /**
     * Sets the compression level for message payloads.
     *
     * @param compressLevel the level of compression to be applied, typically ranging from 1 (fastest, least compression)
     *                      to 9 (slowest, most compression)
     */
    public void setCompressLevel(int compressLevel) {
        this.compressLevel = compressLevel;
    }

    /**
     * Retrieves the configured compression type for the message producer.
     *
     * @return the configured {@link CompressionType}, indicating the type of compression used.
     */
    public CompressionType getCompressionType() {
        return compressionType;
    }

    /**
     * Sets the compression type for the producer configuration.
     *
     * @param compressionType the compression type to be used by the message producer
     */
    public void setCompressionType(CompressionType compressionType) {
        this.compressionType = compressionType;
    }
}
