package org.ostara.common;

import java.util.Objects;

public class TopicPartition {
    private String topic;
    private int partition;

    public TopicPartition(String topic, int partition) {
        this.topic = topic;
        this.partition = partition;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TopicPartition that = (TopicPartition) o;
        return partition == that.partition && Objects.equals(topic, that.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, partition);
    }

    @Override
    public String toString() {
        return "TopicPartition{" +
                "topic='" + topic + '\'' +
                ", partition=" + partition +
                '}';
    }
}
