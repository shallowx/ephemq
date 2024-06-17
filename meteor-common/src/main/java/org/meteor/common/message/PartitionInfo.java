package org.meteor.common.message;

import java.util.Objects;
import java.util.Set;

public class PartitionInfo {
    private final String topic;
    private final int partition;
    private final int ledger;
    private final int epoch;
    private final String leader;
    private final Set<String> replicas;
    private final TopicConfig topicConfig;
    private final int version;
    private int topicId;

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

    @Override
    public String toString() {
        return "(" +
                "topic='" + topic + '\'' +
                ", topicId=" + topicId +
                ", partition=" + partition +
                ", ledger=" + ledger +
                ", epoch=" + epoch +
                ", leader='" + leader + '\'' +
                ", replicas=" + replicas +
                ", topicConfig=" + topicConfig +
                ", version=" + version +
                ')';
    }
}
