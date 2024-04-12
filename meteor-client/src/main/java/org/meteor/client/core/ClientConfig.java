package org.meteor.client.core;

import io.netty.util.NettyRuntime;
import java.util.ArrayList;
import java.util.List;

public class ClientConfig {
    private List<String> bootstrapAddresses = new ArrayList<>();
    private boolean socketEpollPrefer = true;
    private int socketSendBufferSize = 65536;
    private int socketReceiveBufferSize = 65536;
    private int channelConnectionTimeoutMilliseconds = 3000;
    private int channelKeepPeriodMilliseconds = 20000;
    private int channelIdleTimeoutMilliseconds = 30000;
    private int channelInvokePermits = 100;
    private int workerThreadLimit = NettyRuntime.availableProcessors();
    private int metadataTimeoutMilliseconds = 5000;
    private int metadataRefreshPeriodMilliseconds = 5000;
    private int connectionPoolCapacity = 1;
    private int createTopicTimeoutMilliseconds = 2000;
    private int deleteTopicTimeoutMilliseconds = 2000;
    private int calculatePartitionsTimeoutMilliseconds = 5000;

    private int migrateLedgerTimeoutMilliseconds = 5000;

    public int getMigrateLedgerTimeoutMilliseconds() {
        return migrateLedgerTimeoutMilliseconds;
    }

    public void setMigrateLedgerTimeoutMilliseconds(int migrateLedgerTimeoutMilliseconds) {
        this.migrateLedgerTimeoutMilliseconds = migrateLedgerTimeoutMilliseconds;
    }

    public int getCalculatePartitionsTimeoutMilliseconds() {
        return calculatePartitionsTimeoutMilliseconds;
    }

    public void setCalculatePartitionsTimeoutMilliseconds(int calculatePartitionsTimeoutMilliseconds) {
        this.calculatePartitionsTimeoutMilliseconds = calculatePartitionsTimeoutMilliseconds;
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

    public int getChannelConnectionTimeoutMilliseconds() {
        return channelConnectionTimeoutMilliseconds;
    }

    public void setChannelConnectionTimeoutMilliseconds(int channelConnectionTimeoutMilliseconds) {
        this.channelConnectionTimeoutMilliseconds = channelConnectionTimeoutMilliseconds;
    }

    public int getChannelKeepPeriodMilliseconds() {
        return channelKeepPeriodMilliseconds;
    }

    public void setChannelKeepPeriodMilliseconds(int channelKeepPeriodMilliseconds) {
        this.channelKeepPeriodMilliseconds = channelKeepPeriodMilliseconds;
    }

    public int getChannelIdleTimeoutMilliseconds() {
        return channelIdleTimeoutMilliseconds;
    }

    public void setChannelIdleTimeoutMilliseconds(int channelIdleTimeoutMilliseconds) {
        this.channelIdleTimeoutMilliseconds = channelIdleTimeoutMilliseconds;
    }

    public int getChannelInvokePermits() {
        return channelInvokePermits;
    }

    public void setChannelInvokePermits(int channelInvokePermits) {
        this.channelInvokePermits = channelInvokePermits;
    }

    public int getWorkerThreadLimit() {
        return workerThreadLimit;
    }

    public void setWorkerThreadLimit(int workerThreadLimit) {
        this.workerThreadLimit = workerThreadLimit;
    }

    public int getMetadataTimeoutMilliseconds() {
        return metadataTimeoutMilliseconds;
    }

    public void setMetadataTimeoutMilliseconds(int metadataTimeoutMilliseconds) {
        this.metadataTimeoutMilliseconds = metadataTimeoutMilliseconds;
    }

    public int getMetadataRefreshPeriodMilliseconds() {
        return metadataRefreshPeriodMilliseconds;
    }

    public void setMetadataRefreshPeriodMilliseconds(int metadataRefreshPeriodMilliseconds) {
        this.metadataRefreshPeriodMilliseconds = metadataRefreshPeriodMilliseconds;
    }

    public int getConnectionPoolCapacity() {
        return connectionPoolCapacity;
    }

    public void setConnectionPoolCapacity(int connectionPoolCapacity) {
        this.connectionPoolCapacity = connectionPoolCapacity;
    }

    public int getCreateTopicTimeoutMilliseconds() {
        return createTopicTimeoutMilliseconds;
    }

    public void setCreateTopicTimeoutMilliseconds(int createTopicTimeoutMilliseconds) {
        this.createTopicTimeoutMilliseconds = createTopicTimeoutMilliseconds;
    }

    public int getDeleteTopicTimeoutMilliseconds() {
        return deleteTopicTimeoutMilliseconds;
    }

    public void setDeleteTopicTimeoutMilliseconds(int deleteTopicTimeoutMilliseconds) {
        this.deleteTopicTimeoutMilliseconds = deleteTopicTimeoutMilliseconds;
    }
}
