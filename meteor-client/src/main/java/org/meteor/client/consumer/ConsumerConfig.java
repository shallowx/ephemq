package org.meteor.client.consumer;

import io.netty.util.NettyRuntime;
import org.meteor.client.core.ClientConfig;

public class ConsumerConfig {
    private ClientConfig clientConfig = new ClientConfig();
    private int controlTimeoutMilliseconds = 5000;
    private int controlRetryDelayMilliseconds = 2000;
    private int handlerThreadLimit = NettyRuntime.availableProcessors();
    private int handlerShardLimit = handlerThreadLimit * 10;
    private int handlerPendingLimit = 100;

    public ClientConfig getClientConfig() {
        return clientConfig;
    }

    public void setClientConfig(ClientConfig clientConfig) {
        this.clientConfig = clientConfig;
    }

    public int getControlTimeoutMillis() {
        return controlTimeoutMilliseconds;
    }

    public void setControlTimeoutMillis(int controlTimeoutMilliseconds) {
        this.controlTimeoutMilliseconds = controlTimeoutMilliseconds;
    }

    public int getControlRetryDelayMillis() {
        return controlRetryDelayMilliseconds;
    }

    public void setControlRetryDelayMillis(int controlRetryDelayMilliseconds) {
        this.controlRetryDelayMilliseconds = controlRetryDelayMilliseconds;
    }

    public int getHandlerThreads() {
        return handlerThreadLimit;
    }

    public void setHandlerThreads(int handlerThreadLimit) {
        this.handlerThreadLimit = handlerThreadLimit;
    }

    public int getHandlerShards() {
        return handlerShardLimit;
    }

    public void setHandlerShards(int handlerShardLimit) {
        this.handlerShardLimit = handlerShardLimit;
    }

    public int getHandlerPendings() {
        return handlerPendingLimit;
    }

    public void setHandlerPendings(int handlerPendingLimit) {
        this.handlerPendingLimit = handlerPendingLimit;
    }
}
