package org.ostara.client.producer;

import org.ostara.client.ClientConfig;

public class ProducerConfig {
    private ClientConfig clientConfig = new ClientConfig();

    private int sendTimeoutMs = 2000;
    private int sendAsyncTimeoutMs = 2000;
    private int sendOnewayTimeoutMs = 2000;

    public ClientConfig getClientConfig() {
        return clientConfig;
    }

    public void setClientConfig(ClientConfig clientConfig) {
        this.clientConfig = clientConfig;
    }

    public int getSendTimeoutMs() {
        return sendTimeoutMs;
    }

    public void setSendTimeoutMs(int sendTimeoutMs) {
        this.sendTimeoutMs = sendTimeoutMs;
    }

    public int getSendAsyncTimeoutMs() {
        return sendAsyncTimeoutMs;
    }

    public void setSendAsyncTimeoutMs(int sendAsyncTimeoutMs) {
        this.sendAsyncTimeoutMs = sendAsyncTimeoutMs;
    }

    public int getSendOnewayTimeoutMs() {
        return sendOnewayTimeoutMs;
    }

    public void setSendOnewayTimeoutMs(int sendOnewayTimeoutMs) {
        this.sendOnewayTimeoutMs = sendOnewayTimeoutMs;
    }
}
