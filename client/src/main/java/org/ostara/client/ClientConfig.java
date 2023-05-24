package org.ostara.client;

import java.util.ArrayList;
import java.util.List;

public class ClientConfig {
    private List<String> bootstrapAddresses = new ArrayList<>();

    private boolean socketEpollPrefer = true;
    private int socketSendBufferSize = 65536;
    private int socketReceiveBufferSize = 65536;
    private int channelConnectionTimeoutMs = 3000;
    private int channelKeepPeriodMs = 20000;
    private int channelIdleTimeoutMs = 30000;
    private int channelInvokePermits = 100;
    private int workerThreadCount = Runtime.getRuntime().availableProcessors();
    private int metadataTimeoutMs = 5000;
    private int metadataRefreshPeriodMs = 5000;
    private int connectionPoolCapacity = 1;
    private int createTopicTimeoutMs = 2000;
    private int deleteTopicTimeoutMs = 2000;
    private int calculatePartitionsTimeoutMs = 5000;

    private int migrateLedgerTimeoutMs = 5000;

    public int getMigrateLedgerTimeoutMs() {
        return migrateLedgerTimeoutMs;
    }

    public void setMigrateLedgerTimeoutMs(int migrateLedgerTimeoutMs) {
        this.migrateLedgerTimeoutMs = migrateLedgerTimeoutMs;
    }

    public int getCalculatePartitionsTimeoutMs() {
        return calculatePartitionsTimeoutMs;
    }

    public void setCalculatePartitionsTimeoutMs(int calculatePartitionsTimeoutMs) {
        this.calculatePartitionsTimeoutMs = calculatePartitionsTimeoutMs;
    }

    public List<String> getBootstrapAddresses() {
        return bootstrapAddresses;
    }

    public void setBootstrapAddresses(List<String> bootstrapAddresses) {
        this.bootstrapAddresses = bootstrapAddresses;
    }

    public boolean isSocketEpollPrefer() {
        return socketEpollPrefer;
    }

    public void setSocketEpollPrefer(boolean socketEpollPrefer) {
        this.socketEpollPrefer = socketEpollPrefer;
    }

    public int getSocketSendBufferSize() {
        return socketSendBufferSize;
    }

    public void setSocketSendBufferSize(int socketSendBufferSize) {
        this.socketSendBufferSize = socketSendBufferSize;
    }

    public int getSocketReceiveBufferSize() {
        return socketReceiveBufferSize;
    }

    public void setSocketReceiveBufferSize(int socketReceiveBufferSize) {
        this.socketReceiveBufferSize = socketReceiveBufferSize;
    }

    public int getChannelConnectionTimeoutMs() {
        return channelConnectionTimeoutMs;
    }

    public void setChannelConnectionTimeoutMs(int channelConnectionTimeoutMs) {
        this.channelConnectionTimeoutMs = channelConnectionTimeoutMs;
    }

    public int getChannelKeepPeriodMs() {
        return channelKeepPeriodMs;
    }

    public void setChannelKeepPeriodMs(int channelKeepPeriodMs) {
        this.channelKeepPeriodMs = channelKeepPeriodMs;
    }

    public int getChannelIdleTimeoutMs() {
        return channelIdleTimeoutMs;
    }

    public void setChannelIdleTimeoutMs(int channelIdleTimeoutMs) {
        this.channelIdleTimeoutMs = channelIdleTimeoutMs;
    }

    public int getChannelInvokePermits() {
        return channelInvokePermits;
    }

    public void setChannelInvokePermits(int channelInvokePermits) {
        this.channelInvokePermits = channelInvokePermits;
    }

    public int getWorkerThreadCount() {
        return workerThreadCount;
    }

    public void setWorkerThreadCount(int workerThreadCount) {
        this.workerThreadCount = workerThreadCount;
    }

    public int getMetadataTimeoutMs() {
        return metadataTimeoutMs;
    }

    public void setMetadataTimeoutMs(int metadataTimeoutMs) {
        this.metadataTimeoutMs = metadataTimeoutMs;
    }

    public int getMetadataRefreshPeriodMs() {
        return metadataRefreshPeriodMs;
    }

    public void setMetadataRefreshPeriodMs(int metadataRefreshPeriodMs) {
        this.metadataRefreshPeriodMs = metadataRefreshPeriodMs;
    }

    public int getConnectionPoolCapacity() {
        return connectionPoolCapacity;
    }

    public void setConnectionPoolCapacity(int connectionPoolCapacity) {
        this.connectionPoolCapacity = connectionPoolCapacity;
    }

    public int getCreateTopicTimeoutMs() {
        return createTopicTimeoutMs;
    }

    public void setCreateTopicTimeoutMs(int createTopicTimeoutMs) {
        this.createTopicTimeoutMs = createTopicTimeoutMs;
    }

    public int getDeleteTopicTimeoutMs() {
        return deleteTopicTimeoutMs;
    }

    public void setDeleteTopicTimeoutMs(int deleteTopicTimeoutMs) {
        this.deleteTopicTimeoutMs = deleteTopicTimeoutMs;
    }
}
