package org.meteor.client.consumer;

import io.netty.util.NettyRuntime;
import org.meteor.client.core.ClientConfig;
import org.meteor.common.compression.CompressionType;

public class ConsumerConfig {
    private ClientConfig clientConfig = new ClientConfig();
    private int controlTimeoutMilliseconds = 5000;
    private int controlRetryDelayMilliseconds = 2000;
    private int handlerThreadLimit = NettyRuntime.availableProcessors();
    private int handlerShardLimit = handlerThreadLimit * 10;
    private int handlerPendingLimit = 100;
    private CompressionType compressionType;

    public ClientConfig getClientConfig() {
        return clientConfig;
    }

    public void setClientConfig(ClientConfig clientConfig) {
        this.clientConfig = clientConfig;
    }

    public int getControlTimeoutMilliseconds() {
        return controlTimeoutMilliseconds;
    }

    public void setControlTimeoutMilliseconds(int controlTimeoutMilliseconds) {
        this.controlTimeoutMilliseconds = controlTimeoutMilliseconds;
    }

    public int getControlRetryDelayMilliseconds() {
        return controlRetryDelayMilliseconds;
    }

    public void setControlRetryDelayMilliseconds(int controlRetryDelayMilliseconds) {
        this.controlRetryDelayMilliseconds = controlRetryDelayMilliseconds;
    }

    public int getHandlerThreadLimit() {
        return handlerThreadLimit;
    }

    public void setHandlerThreadLimit(int handlerThreadLimit) {
        this.handlerThreadLimit = handlerThreadLimit;
    }

    public int getHandlerShardLimit() {
        return handlerShardLimit;
    }

    public void setHandlerShardLimit(int handlerShardLimit) {
        this.handlerShardLimit = handlerShardLimit;
    }

    public int getHandlerPendingLimit() {
        return handlerPendingLimit;
    }

    public void setHandlerPendingLimit(int handlerPendingLimit) {
        this.handlerPendingLimit = handlerPendingLimit;
    }

    public CompressionType getCompressionType() {
        return compressionType;
    }

    public void setCompressionType(CompressionType compressionType) {
        this.compressionType = compressionType;
    }
}
