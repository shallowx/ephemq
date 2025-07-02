package org.meteor.client.core;

import javax.annotation.Nonnull;
import java.net.SocketAddress;
import java.util.List;
import java.util.Objects;

/**
 * Represents a ledger of messages in a distributed messaging system.
 * A MessageLedger object holds information about a specific partition
 * of a topic, including its identifier, version, leader and participant addresses,
 * topic name, and partition number.
 *
 * @param id           the unique identifier of the ledger.
 * @param version      the version of the ledger.
 * @param leader       the network address of the leader node for this partition.
 * @param participants a list of network addresses of the participant nodes.
 * @param topic        the name of the topic to which this ledger belongs.
 * @param partition    the partition number within the topic.
 */
public record MessageLedger(int id, int version, SocketAddress leader, List<SocketAddress> participants, String topic,
                            int partition) {

    /**
     * Compares this MessageLedger to the specified object for equality.
     * The result is true if and only if the argument is not null,
     * is a MessageLedger object, and the id fields of both objects are equal.
     *
     * @param o the object to compare this MessageLedger against.
     * @return true if the given object represents an equivalent MessageLedger, false otherwise.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MessageLedger that = (MessageLedger) o;
        return id == that.id;
    }

    /**
     * Computes a hash code for the MessageLedger object.
     *
     * @return a hash code value for this object, which is based on the id field.
     */
    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    /**
     * Provides a string representation of the MessageLedger object.
     *
     * @return a string that includes the id, version, leader, participants, topic, and partition of the ledger.
     */
    @Override
    @Nonnull
    public String toString() {
        return "MessageLedger (id=%d, version=%d, leader=%s, participants=%s, topic='%s', partition=%d)".formatted(id, version, leader, participants, topic, partition);
    }
}
