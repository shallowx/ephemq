package org.ostara.client.consumer;

import org.ostara.client.internal.ClientConfig;

public class ConsumerConfig {
    private ClientConfig clientConfig = new ClientConfig();
    private int controlTimeoutMs = 3000;
    private int controlRetryDelayMs = 2000;
    private int handlerThreadCount = Runtime.getRuntime().availableProcessors();
    private int handlerShardCount = handlerThreadCount * 10;
    private int handlerPendingCount = 100;

    public ClientConfig getClientConfig() {
        return clientConfig;
    }

    public void setClientConfig(ClientConfig clientConfig) {
        this.clientConfig = clientConfig;
    }

    public int getControlTimeoutMs() {
        return controlTimeoutMs;
    }

    public void setControlTimeoutMs(int controlTimeoutMs) {
        this.controlTimeoutMs = controlTimeoutMs;
    }

    public int getControlRetryDelayMs() {
        return controlRetryDelayMs;
    }

    public void setControlRetryDelayMs(int controlRetryDelayMs) {
        this.controlRetryDelayMs = controlRetryDelayMs;
    }

    public int getHandlerThreadCount() {
        return handlerThreadCount;
    }

    public void setHandlerThreadCount(int handlerThreadCount) {
        this.handlerThreadCount = handlerThreadCount;
    }

    public int getHandlerShardCount() {
        return handlerShardCount;
    }

    public void setHandlerShardCount(int handlerShardCount) {
        this.handlerShardCount = handlerShardCount;
    }

    public int getHandlerPendingCount() {
        return handlerPendingCount;
    }

    public void setHandlerPendingCount(int handlerPendingCount) {
        this.handlerPendingCount = handlerPendingCount;
    }
}
