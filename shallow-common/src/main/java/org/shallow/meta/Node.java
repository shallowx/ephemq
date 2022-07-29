package org.shallow.meta;

import java.net.SocketAddress;
import java.util.Objects;

public class Node {
    private int id;
    private String name;
    private SocketAddress socketAddress;
    private long registerTime;

    public Node(int id, String name, SocketAddress socketAddress, long registerTime) {
        this.id = id;
        this.name = name;
        this.socketAddress = socketAddress;
        this.registerTime = registerTime;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
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
        if (!(o instanceof Node)) return false;
        Node node1 = (Node) o;
        return getId() == node1.getId() && getName().equals(node1.getName()) && getSocketAddress().equals(node1.getSocketAddress());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId(), getName(), getSocketAddress());
    }

    @Override
    public String toString() {
        return "Node{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", socketAddress=" + socketAddress +
                ", registerTime=" + registerTime +
                '}';
    }
}
