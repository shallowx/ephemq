package org.shallow.meta;

import java.net.SocketAddress;
import java.util.Objects;

public class Node {
    private String name;
    private SocketAddress socketAddress;
    private long registerTime;

    public Node(String name, SocketAddress socketAddress, long registerTime) {
        this.name = name;
        this.socketAddress = socketAddress;
        this.registerTime = registerTime;
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

    public long getRegisterTime() {
        return registerTime;
    }

    public void setRegisterTime(long registerTime) {
        this.registerTime = registerTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Node node)) return false;
        return Objects.equals(getName(), node.getName()) && Objects.equals(getSocketAddress(), node.getSocketAddress());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName(), getSocketAddress());
    }

    @Override
    public String toString() {
        return "Node{" +
                "name='" + name + '\'' +
                ", socketAddress=" + socketAddress +
                ", registerTime=" + registerTime +
                '}';
    }
}
