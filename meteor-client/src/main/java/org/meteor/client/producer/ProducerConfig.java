package org.meteor.client.producer;

import org.meteor.client.core.ClientConfig;

public class ProducerConfig {
    private ClientConfig clientConfig = new ClientConfig();
    private int sendTimeoutMilliseconds = 2000;
    private int sendAsyncTimeoutMilliseconds = 2000;
    private int sendOnewayTimeoutMilliseconds = 2000;
    private int compressLevel = 5;

    public ClientConfig getClientConfig() {
        return clientConfig;
    }

    public int getCompressLevel() {
        return compressLevel;
    }

    public void setCompressLevel(int compressLevel) {
        this.compressLevel = compressLevel;
    }

    public void setClientConfig(ClientConfig clientConfig) {
        this.clientConfig = clientConfig;
    }

    public int getSendTimeoutMilliseconds() {
        return sendTimeoutMilliseconds;
    }

    public void setSendTimeoutMilliseconds(int sendTimeoutMilliseconds) {
        this.sendTimeoutMilliseconds = sendTimeoutMilliseconds;
    }

    public int getSendAsyncTimeoutMilliseconds() {
        return sendAsyncTimeoutMilliseconds;
    }

    public void setSendAsyncTimeoutMilliseconds(int sendAsyncTimeoutMilliseconds) {
        this.sendAsyncTimeoutMilliseconds = sendAsyncTimeoutMilliseconds;
    }

    public int getSendOnewayTimeoutMilliseconds() {
        return sendOnewayTimeoutMilliseconds;
    }

    public void setSendOnewayTimeoutMilliseconds(int sendOnewayTimeoutMilliseconds) {
        this.sendOnewayTimeoutMilliseconds = sendOnewayTimeoutMilliseconds;
    }
}
