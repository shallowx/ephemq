package org.shallow.meta;

import java.util.Objects;

public class TopicInfo {
    private String topic;
    private PartitionInfo partitionInfo;

    public TopicInfo(String topic, PartitionInfo partitionInfo) {
        this.topic = topic;
        this.partitionInfo = partitionInfo;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public PartitionInfo getPartitionInfo() {
        return partitionInfo;
    }

    public void setPartitionInfo(PartitionInfo partitionInfo) {
        this.partitionInfo = partitionInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TopicInfo topicInfo)) return false;
        return Objects.equals(topic, topicInfo.topic) && Objects.equals(partitionInfo, topicInfo.partitionInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, partitionInfo);
    }

    @Override
    public String toString() {
        return "TopicInfo{" +
                "topic='" + topic + '\'' +
                ", partitionInfo=" + partitionInfo +
                '}';
    }
}
