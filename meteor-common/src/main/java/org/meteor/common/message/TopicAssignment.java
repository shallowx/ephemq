package org.meteor.common.message;

import java.util.Set;

/**
 * TopicAssignment class encapsulates the metadata associated with a partition
 * of a topic, providing details about its current status and configuration.
 */
public class TopicAssignment {
    /**
     * Represents the topic of the TopicAssignment.
     * This variable stores the name of the topic that is being managed.
     */
    private String topic;
    /**
     * Unique identifier for the ledger associated with a Kafka topic partition.
     */
    private int ledgerId;
    /**
     * The current epoch of the topic assignment.
     * <p>
     * This variable holds the epoch value which is used to track
     * the version of the topic assignment. It is initialized to -1
     * indicating that no valid epoch is set by default.
     */
    private int epoch = -1;
    /**
     * The partition number within the topic.
     */
    private int partition;
    /**
     * A set that holds the replica nodes for a particular topic partition.
     */
    private Set<String> replicas;
    /**
     * The identifier of the current leader for the topic partition. It typically represents the broker
     * that is currently acting as the leader, managing read and write operations for the partition.
     */
    private String leader;
    /**
     * Holds the configuration settings for a topic assignment.
     */
    private TopicConfig config;
    /**
     * Represents the leader of the topic during transitional states.
     * <p>
     * This value is used when the leadership of the topic is in the process
     * of being transferred or changed. It holds the identifier for the
     * transitional leader, which may not be the final permanent leader.
     */
    private String transitionalLeader;
    /**
     * Represents the version of the topic assignment.
     * This field is transient and will not be serialized.
     */
    private transient int version;

    /**
     *
     */
    public String getTopic() {
        return topic;
    }

    /**
     * Sets the topic name for a TopicAssignment instance.
     *
     * @param topic the name of the topic to be set
     */
    public void setTopic(String topic) {
        this.topic = topic;
    }

    /**
     * Retrieves the ledger ID associated with the topic assignment.
     *
     * @return The ledger ID.
     */
    public int getLedgerId() {
        return ledgerId;
    }

    /**
     * Sets the ledger ID for this topic assignment.
     *
     * @param ledgerId the ledger ID to be set
     */
    public void setLedgerId(int ledgerId) {
        this.ledgerId = ledgerId;
    }

    /**
     * Retrieves the epoch value associated with the topic assignment.
     *
     * @return The epoch value as an integer.
     */
    public int getEpoch() {
        return epoch;
    }

    /**
     * Sets the epoch value for the TopicAssignment.
     *
     * @param epoch The epoch value to set.
     */
    public void setEpoch(int epoch) {
        this.epoch = epoch;
    }

    /**
     * Retrieves the partition ID associated with the current `TopicAssignment`.
     *
     * @return the partition ID.
     */
    public int getPartition() {
        return partition;
    }

    /**
     * Sets the partition identifier for the current object.
     *
     * @param partition the partition id to be set
     */
    public void setPartition(int partition) {
        this.partition = partition;
    }

    /**
     * Retrieves the set of replica identifiers for the topic partition.
     *
     * @return A set containing the identifiers of the replicas.
     */
    public Set<String> getReplicas() {
        return replicas;
    }

    /**
     * Sets the replicas for the topic partition assignment.
     *
     * @param replicas a set of replica node IDs to be assigned to the partition
     */
    public void setReplicas(Set<String> replicas) {
        this.replicas = replicas;
    }

    /**
     * Retrieves the leader of the topic assignment.
     *
     * @return the current leader as a String.
     */
    public String getLeader() {
        return leader;
    }

    /**
     * Sets the leader for the topic assignment.
     *
     * @param leader the identifier of the new leader
     */
    public void setLeader(String leader) {
        this.leader = leader;
    }

    /**
     * Retrieves the configuration settings for a topic.
     *
     * @return the configuration settings of the topic.
     */
    public TopicConfig getConfig() {
        return config;
    }

    /**
     * Sets the configuration for the topic.
     *
     * @param config the configuration to be set
     */
    public void setConfig(TopicConfig config) {
        this.config = config;
    }

    /**
     * Retrieves the current transitional leader of the topic partition.
     *
     * @return the transitional leader's identifier as a string.
     */
    public String getTransitionalLeader() {
        return transitionalLeader;
    }

    /**
     * Sets the transitional leader for this topic assignment.
     *
     * @param transitionalLeader the transitional leader to set
     */
    public void setTransitionalLeader(String transitionalLeader) {
        this.transitionalLeader = transitionalLeader;
    }

    /**
     * Retrieves the version of the topic assignment.
     *
     * @return the version number of the topic assignment
     */
    public int getVersion() {
        return version;
    }

    /**
     * Sets the version of the topic assignment.
     *
     * @param version the new version to be set for the topic assignment
     */
    public void setVersion(int version) {
        this.version = version;
    }

    /**
     * Returns a string representation of the TopicAssignment object.
     *
     * @return a string representation of the TopicAssignment object, containing
     * the values of its fields.
     */
    @Override
    public String toString() {
        return "TopicAssignment (topic='%s', ledgerId=%d, epoch=%d, partition=%d, replicas=%s, leader='%s', config=%s, transitionalLeader='%s', version=%d)".formatted(topic, ledgerId, epoch, partition, replicas, leader, config, transitionalLeader, version);
    }
}
