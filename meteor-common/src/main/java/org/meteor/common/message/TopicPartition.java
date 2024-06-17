package org.meteor.common.message;

import java.util.Objects;

public record TopicPartition(String topic, int partition) {
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TopicPartition that = (TopicPartition) o;
        return partition == that.partition && Objects.equals(topic, that.topic);
    }

    @Override
    public String toString() {
        return "(" +
                "topic='" + topic + '\'' +
                ", partition=" + partition +
                ')';
    }
}
