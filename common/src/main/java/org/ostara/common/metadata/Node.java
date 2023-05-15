package org.ostara.common.metadata;

import java.net.SocketAddress;
import java.util.Objects;

public class Node {
    private String cluster;
    private String name;
    private SocketAddress socketAddress;
    private long lastKeepLiveTime;
    private String state;
    private int hash;

    private Node() {
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

    public void updateState(String state) {
        this.state = state;
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

        private int hash;

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

        public Node build() {
            Node record = new Node();

            record.cluster = this.cluster;
            record.name = this.name;
            record.socketAddress = this.socketAddress;
            record.lastKeepLiveTime = lastKeepLiveTime;
            record.state = this.state;
            record.hash = Objects.hash(cluster, name, socketAddress);
            return record;
        }

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Node that)) {
            return false;
        }
        return getCluster().equals(that.getCluster()) &&
                getName().equals(that.getName()) &&
                getSocketAddress().equals(that.getSocketAddress());
    }

    @Override
    public int hashCode() {
        return hash;
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
