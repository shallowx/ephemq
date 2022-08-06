package org.shallow;

import java.util.LinkedList;
import java.util.List;

public class ClientConfig {
    private List<String> bootstrapSocketAddress = new LinkedList<>();

    private boolean isEpollPrefer = false;
    private int workThreadWholes = availableProcessor();
    private long connectTimeOutMs = 5000;
    private int channelPoolCapacity = 1;
    private int dnsTtlMinExpiredMs = 30;
    private int dnsTtlMaxExpiredSeconds = 300;
    private int negativeTtlSeconds = 30;
    private int ChannelInvokerSemaphore = 2000;
    private int defaultInvokeExpiredMs = 10000;

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

    public int getChannelPoolCapacity() {
        return channelPoolCapacity;
    }

    public void setChannelPoolCapacity(int channelPoolCapacity) {
        this.channelPoolCapacity = channelPoolCapacity;
    }

    public long getConnectTimeOutMs() {
        return connectTimeOutMs;
    }

    public void setConnectTimeOutMs(long connectTimeOutMs) {
        this.connectTimeOutMs = connectTimeOutMs;
    }

    public int getWorkThreadWholes() {
        return workThreadWholes;
    }

    public void setWorkThreadWholes(int workThreadWholes) {
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

    public int getDefaultInvokeExpiredMs() {
        return defaultInvokeExpiredMs;
    }

    public void setDefaultInvokeExpiredMs(int defaultInvokeExpiredMs) {
        this.defaultInvokeExpiredMs = defaultInvokeExpiredMs;
    }
}
