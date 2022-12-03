package org.ostara.client.producer;

import org.ostara.client.internal.ClientConfig;

public class ProducerConfig {

    private ClientConfig clientConfig;

    private int sendTimeoutMs = 5000;
    private int sendOnewayTimeoutMs = 5000;
    private int sendAsyncTimeoutMs = 5000;

    public ClientConfig getClientConfig() {
        return clientConfig;
    }

    public void setClientConfig(ClientConfig clientConfig) {
        this.clientConfig = clientConfig;
    }

    public void setSendTimeoutMs(int sendTimeoutMs) {
        this.sendTimeoutMs = sendTimeoutMs;
    }

    public void setSendOnewayTimeoutMs(int sendOnewayTimeoutMs) {
        this.sendOnewayTimeoutMs = sendOnewayTimeoutMs;
    }

    public void setSendAsyncTimeoutMs(int sendAsyncTimeoutMs) {
        this.sendAsyncTimeoutMs = sendAsyncTimeoutMs;
    }

    public int getSendTimeoutMs() {
        return sendTimeoutMs;
    }

    public int getSendOnewayTimeoutMs() {
        return sendOnewayTimeoutMs;
    }

    public int getSendAsyncTimeoutMs() {
        return sendAsyncTimeoutMs;
    }
}
