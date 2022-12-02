package org.leopard.client.consumer;

import org.leopard.client.internal.ClientConfig;

public class ConsumerConfig {

    private ClientConfig clientConfig;
    public int subscribeInvokeTimeMs = 5000;
    public int cleanSubscribeInvokeTimeMs = 5000;
    public int messageHandleThreadLimit = Runtime.getRuntime().availableProcessors();
    public int messageHandleSemaphoreLimit = 100;

    public ClientConfig getClientConfig() {
        return clientConfig;
    }

    public void setClientConfig(ClientConfig clientConfig) {
        this.clientConfig = clientConfig;
    }

    public int getSubscribeInvokeTimeMs() {
        return subscribeInvokeTimeMs;
    }

    public void setSubscribeInvokeTimeMs(int pushSubscribeInvokeTimeMs) {
        this.subscribeInvokeTimeMs = pushSubscribeInvokeTimeMs;
    }

    public int getCleanSubscribeInvokeTimeMs() {
        return cleanSubscribeInvokeTimeMs;
    }

    public void setCleanSubscribeInvokeTimeMs(int cleanSubscribeInvokeTimeMs) {
        this.cleanSubscribeInvokeTimeMs = cleanSubscribeInvokeTimeMs;
    }

    public int getMessageHandleThreadLimit() {
        return messageHandleThreadLimit;
    }

    public void setMessageHandleThreadLimit(int messageHandleThreadLimit) {
        this.messageHandleThreadLimit = messageHandleThreadLimit;
    }

    public int getMessageHandleSemaphoreLimit() {
        return messageHandleSemaphoreLimit;
    }

    public void setMessageHandleSemaphoreLimit(int messageHandleSemaphoreLimit) {
        this.messageHandleSemaphoreLimit = messageHandleSemaphoreLimit;
    }
}
