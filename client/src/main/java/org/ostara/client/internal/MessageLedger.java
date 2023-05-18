package org.ostara.client.internal;

import java.net.SocketAddress;
import java.util.List;
import java.util.Objects;

public class MessageLedger {
    private int id;
    private int version;
    private SocketAddress leader;
    private List<SocketAddress> replicas;
    private String topic;
    private int partition;

    public MessageLedger(int id, int version, SocketAddress leader, List<SocketAddress> replicas, String topic, int partition) {
        this.id = id;
        this.version = version;
        this.leader = leader;
        this.replicas = replicas;
        this.topic = topic;
        this.partition = partition;
    }

    public int id() {
        return id;
    }

    public int version() {
        return version;
    }

    public SocketAddress leader() {
        return leader;
    }

    public List<SocketAddress> replicas() {
        return replicas;
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MessageLedger that = (MessageLedger) o;
        return id == that.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
