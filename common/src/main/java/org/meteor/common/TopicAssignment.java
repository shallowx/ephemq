package org.meteor.common;

import java.util.Set;

public class TopicAssignment {
    private String topic;
    private int ledgerId;
    private int epoch;
    private int partition;
    private Set<String> replicas;
    private String leader;
    private TopicConfig config;
    private String transitionalLeader;
    private transient int version;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getLedgerId() {
        return ledgerId;
    }

    public void setLedgerId(int ledgerId) {
        this.ledgerId = ledgerId;
    }

    public int getEpoch() {
        return epoch;
    }

    public void setEpoch(int epoch) {
        this.epoch = epoch;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public Set<String> getReplicas() {
        return replicas;
    }

    public void setReplicas(Set<String> replicas) {
        this.replicas = replicas;
    }

    public String getLeader() {
        return leader;
    }

    public void setLeader(String leader) {
        this.leader = leader;
    }

    public TopicConfig getConfig() {
        return config;
    }

    public void setConfig(TopicConfig config) {
        this.config = config;
    }

    public String getTransitionalLeader() {
        return transitionalLeader;
    }

    public void setTransitionalLeader(String transitionalLeader) {
        this.transitionalLeader = transitionalLeader;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    @Override
    public String toString() {
        return "topic_assignment {" +
                "topic='" + topic + '\'' +
                ", ledgerId=" + ledgerId +
                ", epoch=" + epoch +
                ", partition=" + partition +
                ", replicas=" + replicas.toString() +
                ", leader='" + leader + '\'' +
                ", config=" + config +
                ", transitionalLeader='" + transitionalLeader + '\'' +
                ", version=" + version +
                '}';
    }
}
