package org.meteor.client.internal;

import java.net.SocketAddress;
import java.util.List;
import java.util.Objects;

public class MessageLedger {
    private final int id;
    private final int version;
    private final SocketAddress leader;
    private final List<SocketAddress> replicas;
    private final String topic;
    private final int partition;

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

    @Override
    public String toString() {
        return "MessageLedger{" +
                "id=" + id +
                ", version=" + version +
                ", leader=" + leader +
                ", replicas=" + replicas +
                ", topic='" + topic + '\'' +
                ", partition=" + partition +
                '}';
    }
}
