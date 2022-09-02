package org.shallow.consumer;

import org.shallow.ClientConfig;

public class ConsumerConfig {

    private ClientConfig clientConfig;
    public int pullInvokeTimeMs = 5000;
    public int pushSubscribeInvokeTimeMs = 5000;
    public int pushCleanSubscribeInvokeTimeMs = 5000;

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
}
