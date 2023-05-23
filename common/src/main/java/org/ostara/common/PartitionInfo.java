package org.ostara.common;

import java.util.Objects;
import java.util.Set;

public class PartitionInfo {
    private String topic;
    private int topicId;
    private int partition;
    private int ledger;
    private int epoch;
    private String leader;
    private Set<String> replicas;
    private TopicConfig topicConfig;
    private int version;

    public PartitionInfo(String topic, int topicId, int partition, int ledger, int epoch, String leader,
                         Set<String> replicas, TopicConfig topicConfig, int version) {
        this.topic = topic;
        this.topicId = topicId;
        this.partition = partition;
        this.ledger = ledger;
        this.epoch = epoch;
        this.leader = leader;
        this.replicas = replicas;
        this.topicConfig = topicConfig;
        this.version = version;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public int getLedger() {
        return ledger;
    }

    public int getEpoch() {
        return epoch;
    }

    public String getLeader() {
        return leader;
    }

    public Set<String> getReplicas() {
        return replicas;
    }

    public TopicConfig getTopicConfig() {
        return topicConfig;
    }

    public int getVersion() {
        return version;
    }

    public int getTopicId() {
        return topicId;
    }

    public void setTopicId(int topicId) {
        this.topicId = topicId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PartitionInfo that = (PartitionInfo) o;
        return partition == that.partition && ledger == that.ledger && Objects.equals(topic, that.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, partition, ledger);
    }
}
