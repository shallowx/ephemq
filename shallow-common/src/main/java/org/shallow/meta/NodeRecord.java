package org.shallow.meta;

import java.net.SocketAddress;
import java.util.Objects;

public class NodeRecord {
    private String cluster;
    private String name;
    private SocketAddress socketAddress;

    public NodeRecord(String cluster, String name, SocketAddress socketAddress) {
        this.cluster = cluster;
        this.name = name;
        this.socketAddress = socketAddress;
    }

    public String getCluster() {
        return cluster;
    }

    public String getName() {
        return name;
    }

    public void setName(String node) {
        this.name = node;
    }

    public SocketAddress getSocketAddress() {
        return socketAddress;
    }

    public void setSocketAddress(SocketAddress socketAddress) {
        this.socketAddress = socketAddress;
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
                '}';
    }
}
