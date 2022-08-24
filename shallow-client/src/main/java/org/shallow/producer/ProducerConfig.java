package org.shallow.producer;

import org.shallow.ClientConfig;

public class ProducerConfig extends ClientConfig {

    private int sendTimeoutMs = 2000;
    private int sendOnewayTimeoutMs = 2000;
    private int sendAsyncTimeoutMs = 2000;

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
