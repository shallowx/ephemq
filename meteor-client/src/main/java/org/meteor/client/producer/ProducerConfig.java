package org.meteor.client.producer;

import org.meteor.client.internal.ClientConfig;

public class ProducerConfig {
    private ClientConfig clientConfig = new ClientConfig();
    private int sendTimeoutMilliseconds = 2000;
    private int sendAsyncTimeoutMilliseconds = 2000;
    private int sendOnewayTimeoutMilliseconds = 2000;

    public ClientConfig getClientConfig() {
        return clientConfig;
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
