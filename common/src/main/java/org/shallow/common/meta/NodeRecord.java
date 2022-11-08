package org.shallow.common.meta;

import java.net.SocketAddress;
import java.util.Objects;

public class NodeRecord {
    private String cluster;
    private String name;
    private SocketAddress socketAddress;
    private long lastKeepLiveTime;
    private String state;

    private NodeRecord() {
        // unsupported
    }

    public static NodeBuilder newBuilder() {
        return new NodeBuilder();
    }

    public String getCluster() {
        return cluster;
    }

    public String getName() {
        return name;
    }

    public SocketAddress getSocketAddress() {
        return socketAddress;
    }

    public long getLastKeepLiveTime() {
        return lastKeepLiveTime;
    }

    public void updateLastKeepLiveTime(long lastKeepLiveTime) {
        this.lastKeepLiveTime = lastKeepLiveTime;
    }

    public String getState() {
        return state;
    }

    public static class NodeBuilder {
        private String cluster;
        private String name;
        private SocketAddress socketAddress;
        private long lastKeepLiveTime;
        private String state;

        private NodeBuilder() {
        }

        public NodeBuilder cluster(String cluster) {
            this.cluster = cluster;
            return this;
        }

        public NodeBuilder name(String name) {
            this.name = name;
            return this;
        }

        public NodeBuilder socketAddress(SocketAddress socketAddress) {
            this.socketAddress = socketAddress;
            return this;
        }

        public NodeBuilder lastKeepLiveTime(long lastKeepLiveTime) {
            this.lastKeepLiveTime = lastKeepLiveTime;
            return this;
        }

        public NodeBuilder state(String state) {
            this.state = state;
            return this;
        }

        public NodeRecord build() {
            NodeRecord record = new NodeRecord();

            record.cluster = this.cluster;
            record.name = this.name;
            record.socketAddress = this.socketAddress;
            record.lastKeepLiveTime = lastKeepLiveTime;
            record.state = this.state;

            return record;
        }

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NodeRecord that)) return false;
        return getCluster().equals(that.getCluster()) &&
                getName().equals(that.getName()) &&
                getSocketAddress().equals(that.getSocketAddress());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getCluster(), getName(), getSocketAddress());
    }

    @Override
    public String toString() {
        return "NodeRecord{" +
                "cluster='" + cluster + '\'' +
                ", name='" + name + '\'' +
                ", socketAddress=" + socketAddress +
                ", state=" + state +
                ", lastKeepLiveTime=" + lastKeepLiveTime +
                '}';
    }
}
