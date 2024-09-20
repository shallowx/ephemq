package org.meteor.client.consumer;

import io.netty.util.NettyRuntime;
import org.meteor.client.core.ClientConfig;

/**
 * Configuration class for a consumer.
 * <p>
 * This class is used to configure various parameters for a consumer,
 * such as timeouts, retry delays, thread limits, and other consumer-specific settings.
 */
public class ConsumerConfig {
    /**
     * Holds the configuration settings for the client.
     *
     * This object contains various configurations such as socket settings,
     * connection timeouts, worker thread limits, and more. It is used by
     * the consumer to communicate with backend services.
     */
    private ClientConfig clientConfig = new ClientConfig();
    /**
     * The timeout duration in milliseconds for control operations.
     *
     * This variable specifies the maximum amount of time the consumer will wait
     * for control operations to complete. If the operation does not complete within
     * this time frame, it will timeout and potentially trigger a retry or an error handling mechanism.
     */
    private int controlTimeoutMilliseconds = 5000;
    /**
     * Delay in milliseconds before retrying a control operation if it fails.
     *
     * This variable specifies the time to wait before attempting to retry
     * a control operation that has encountered an error or failure. The default
     * delay is set to 2000 milliseconds (2 seconds).
     */
    private int controlRetryDelayMilliseconds = 2000;
    /**
     * The maximum number of handler threads that can be utilized.
     * This determines the limit based on the number of available processors.
     */
    private int handlerThreadLimit = NettyRuntime.availableProcessors();
    /**
     * The maximum number of shards that the handler is allowed to process simultaneously.
     * This is calculated as 10 times the handlerThreadLimit.
     */
    private int handlerShardLimit = handlerThreadLimit * 10;
    /**
     * The maximum number of pending tasks that a handler can have at any given time.
     *
     * This setting is used to control the number of tasks that can be queued up for
     * processing by the handler threads. If the limit is reached, new tasks may be
     * delayed or rejected until some of the current tasks are processed.
     */
    private int handlerPendingLimit = 100;

    /**
     * Retrieves the current configuration settings for the client.
     *
     * @return the current ClientConfig instance containing the client's configuration settings.
     */
    public ClientConfig getClientConfig() {
        return clientConfig;
    }

    /**
     * Sets the client configuration.
     *
     * @param clientConfig the client configuration to set
     */
    public void setClientConfig(ClientConfig clientConfig) {
        this.clientConfig = clientConfig;
    }

    /**
     * Retrieves the timeout duration in milliseconds used for control operations.
     *
     * @return the timeout duration in milliseconds.
     */
    public int getControlTimeoutMilliseconds() {
        return controlTimeoutMilliseconds;
    }

    /**
     * Sets the timeout duration in milliseconds for control operations.
     *
     * @param controlTimeoutMilliseconds the timeout duration in milliseconds for control operations
     */
    public void setControlTimeoutMilliseconds(int controlTimeoutMilliseconds) {
        this.controlTimeoutMilliseconds = controlTimeoutMilliseconds;
    }

    /**
     * Gets the delay in milliseconds before retrying a failed control operation.
     *
     * @return the control retry delay in milliseconds
     */
    public int getControlRetryDelayMilliseconds() {
        return controlRetryDelayMilliseconds;
    }

    /**
     * Sets the retry delay for control operations in milliseconds.
     *
     * @param controlRetryDelayMilliseconds the delay in milliseconds to wait before retrying a control operation
     */
    public void setControlRetryDelayMilliseconds(int controlRetryDelayMilliseconds) {
        this.controlRetryDelayMilliseconds = controlRetryDelayMilliseconds;
    }

    /**
     * Gets the maximum number of handler threads allowed for processing.
     *
     * @return the maximum number of handler threads.
     */
    public int getHandlerThreadLimit() {
        return handlerThreadLimit;
    }

    /**
     * Sets the limit on the number of handler threads.
     *
     * @param handlerThreadLimit the maximum number of handler threads allowed.
     */
    public void setHandlerThreadLimit(int handlerThreadLimit) {
        this.handlerThreadLimit = handlerThreadLimit;
    }

    /**
     * Returns the limit of handler shards.
     *
     * @return the handler shard limit
     */
    public int getHandlerShardLimit() {
        return handlerShardLimit;
    }

    /**
     * Sets the handler shard limit for the consumer configuration.
     *
     * @param handlerShardLimit the maximum number of shards the handler can process concurrently
     */
    public void setHandlerShardLimit(int handlerShardLimit) {
        this.handlerShardLimit = handlerShardLimit;
    }

    /**
     * Gets the limit on the number of pending handler operations.
     *
     * @return the maximum number of pending handler operations allowed.
     */
    public int getHandlerPendingLimit() {
        return handlerPendingLimit;
    }

    /**
     * Sets the limit for the number of pending handlers.
     *
     * @param handlerPendingLimit the maximum number of pending handlers allowed.
     */
    public void setHandlerPendingLimit(int handlerPendingLimit) {
        this.handlerPendingLimit = handlerPendingLimit;
    }
}
