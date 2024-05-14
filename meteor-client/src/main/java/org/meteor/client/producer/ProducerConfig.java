package org.meteor.client.producer;

import org.meteor.client.core.ClientConfig;
import org.meteor.common.compression.CompressionType;

public class ProducerConfig {
    private ClientConfig clientConfig = new ClientConfig();
    private int sendTimeoutMilliseconds = 2000;
    private int sendAsyncTimeoutMilliseconds = 2000;
    private int sendOnewayTimeoutMilliseconds = 2000;
    private int compressLevel = 5;
    private CompressionType compressionType;
    private boolean enableCompression;

    public boolean isEnableCompression() {
        return enableCompression;
    }

    public void setEnableCompression(boolean enableCompression) {
        this.enableCompression = enableCompression;
    }

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

    public int getCompressLevel() {
        return compressLevel;
    }

    public void setCompressLevel(int compressLevel) {
        this.compressLevel = compressLevel;
    }

    public CompressionType getCompressionType() {
        return compressionType;
    }

    public void setCompressionType(CompressionType compressionType) {
        this.compressionType = compressionType;
    }
}
