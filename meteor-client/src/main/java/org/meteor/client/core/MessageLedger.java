package org.meteor.client.core;

import java.net.SocketAddress;
import java.util.List;
import java.util.Objects;

public record MessageLedger(int id, int version, SocketAddress leader, List<SocketAddress> participants, String topic,
                            int partition) {

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
                ", participants=" + participants +
                ", topic='" + topic + '\'' +
                ", partition=" + partition +
                '}';
    }
}
