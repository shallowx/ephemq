package org.meteor.common.message;

import java.util.Objects;

/**
 * Represents a specific partition of a topic in a messaging system.
 * <p>
 * This class is a record that holds information about which topic
 * and which partition within that topic it represents.
 *
 * @param topic     The topic name.
 * @param partition The partition number.
 */
public record TopicPartition(String topic, int partition) {
    /**
     * Checks if the specified object is equal to this {@code TopicPartition}.
     *
     * @param o the object to compare with this {@code TopicPartition} for equality.
     * @return {@code true} if the specified object is equal to this {@code TopicPartition};
     *         {@code false} otherwise.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TopicPartition that = (TopicPartition) o;
        return partition == that.partition && Objects.equals(topic, that.topic);
    }

    /**
     * Returns a string representation of the TopicPartition.
     *
     * @return a string containing the topic and partition information.
     */
    @Override
    public String toString() {
        return "TopicPartition (topic='%s', partition=%d)".formatted(topic, partition);
    }
}
