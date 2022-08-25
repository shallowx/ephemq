package org.shallow;

import java.util.LinkedList;
import java.util.List;

public class ClientConfig {
    private List<String> bootstrapSocketAddress = new LinkedList<>();

    private boolean isEpollPrefer = true;
    private int workThreadWholes = availableProcessor();
    private int connectTimeOutMs = 5000;
    private int channelFixedPoolCapacity = 1;
    private int dnsTtlMinExpiredMs = 30;
    private int dnsTtlMaxExpiredSeconds = 300;
    private int negativeTtlSeconds = 30;
    private int ChannelInvokerSemaphore = 2000;
    private int invokeExpiredMs = 5000;
    private int refreshMetadataIntervalMs = 5000;
    private long metadataExpiredMs = Long.MAX_VALUE;

    private int availableProcessor() {
        return Runtime.getRuntime().availableProcessors();
    }

    public int getDnsTtlMinExpiredMs() {
        return dnsTtlMinExpiredMs;
    }

    public void setDnsTtlMinExpiredMs(int dnsTtlMinExpiredMs) {
        this.dnsTtlMinExpiredMs = dnsTtlMinExpiredMs;
    }

    public int getDnsTtlMaxExpiredSeconds() {
        return dnsTtlMaxExpiredSeconds;
    }

    public void setDnsTtlMaxExpiredSeconds(int dnsTtlMaxExpiredSeconds) {
        this.dnsTtlMaxExpiredSeconds = dnsTtlMaxExpiredSeconds;
    }

    public int getNegativeTtlSeconds() {
        return negativeTtlSeconds;
    }

    public void setNegativeTtlSeconds(int negativeTtlSeconds) {
        this.negativeTtlSeconds = negativeTtlSeconds;
    }

    public int getConnectTimeOutMs() {
        return connectTimeOutMs;
    }

    public void setConnectTimeOutMs(int connectTimeOutMs) {
        this.connectTimeOutMs = connectTimeOutMs;
    }

    public int getWorkThreadLimit() {
        return workThreadWholes;
    }

    public void setWorkThreadLimit(int workThreadWholes) {
        this.workThreadWholes = workThreadWholes;
    }

    public boolean isEpollPrefer() {
        return isEpollPrefer;
    }

    public void setEpollPrefer(boolean epollPrefer) {
        isEpollPrefer = epollPrefer;
    }

    public List<String> getBootstrapSocketAddress() {
        return bootstrapSocketAddress;
    }

    public void setBootstrapSocketAddress(List<String> bootstrapSocketAddress) {
        this.bootstrapSocketAddress = bootstrapSocketAddress;
    }

    public int getChannelInvokerSemaphore() {
        return ChannelInvokerSemaphore;
    }

    public void setChannelInvokerSemaphore(int channelInvokerSemaphore) {
        ChannelInvokerSemaphore = channelInvokerSemaphore;
    }

    public int getInvokeExpiredMs() {
        return invokeExpiredMs;
    }

    public void setInvokeExpiredMs(int invokeExpiredMs) {
        this.invokeExpiredMs = invokeExpiredMs;
    }

    public int getRefreshMetadataIntervalMs() {
        return refreshMetadataIntervalMs;
    }

    public void setRefreshMetadataIntervalMs(int refreshMetadataIntervalMs) {
        this.refreshMetadataIntervalMs = refreshMetadataIntervalMs;
    }

    public int getChannelFixedPoolCapacity() {
        return channelFixedPoolCapacity;
    }

    public void setChannelFixedPoolCapacity(int channelFixedPoolCapacity) {
        this.channelFixedPoolCapacity = channelFixedPoolCapacity;
    }

    public long getMetadataExpiredMs() {
        return metadataExpiredMs;
    }

    public void setMetadataExpiredMs(long metadataExpiredMs) {
        this.metadataExpiredMs = metadataExpiredMs;
    }
}
