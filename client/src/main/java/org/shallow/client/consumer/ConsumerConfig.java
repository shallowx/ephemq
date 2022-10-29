package org.shallow.client.consumer;

import org.shallow.client.ClientConfig;

public class ConsumerConfig {

    private ClientConfig clientConfig;
    public int pullInvokeTimeMs = 5000;
    public int pushSubscribeInvokeTimeMs = 5000;
    public int pushCleanSubscribeInvokeTimeMs = 5000;
    public int messageHandleThreadLimit = Runtime.getRuntime().availableProcessors();
    public int messageHandleSemaphoreLimit = 100;

    public ClientConfig getClientConfig() {
        return clientConfig;
    }

    public void setClientConfig(ClientConfig clientConfig) {
        this.clientConfig = clientConfig;
    }

    public int getPullInvokeTimeMs() {
        return pullInvokeTimeMs;
    }

    public void setPullInvokeTimeMs(int pullInvokeTimeMs) {
        this.pullInvokeTimeMs = pullInvokeTimeMs;
    }

    public int getPushSubscribeInvokeTimeMs() {
        return pushSubscribeInvokeTimeMs;
    }

    public void setPushSubscribeInvokeTimeMs(int pushSubscribeInvokeTimeMs) {
        this.pushSubscribeInvokeTimeMs = pushSubscribeInvokeTimeMs;
    }

    public int getPushCleanSubscribeInvokeTimeMs() {
        return pushCleanSubscribeInvokeTimeMs;
    }

    public void setPushCleanSubscribeInvokeTimeMs(int pushCleanSubscribeInvokeTimeMs) {
        this.pushCleanSubscribeInvokeTimeMs = pushCleanSubscribeInvokeTimeMs;
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
