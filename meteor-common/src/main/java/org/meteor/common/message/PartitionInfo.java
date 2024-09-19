package org.meteor.common.message;

import java.util.Objects;
import java.util.Set;

/**
 * Represents the metadata information of a partition within a topic.
 */
public class PartitionInfo {
    /**
     * The topic name associated with this partition information.
     * This value uniquely identifies the topic.
     */
    private final String topic;
    /**
     * Represents the partition number within a topic.
     */
    private final int partition;
    /**
     * Represents the ledger identifier for the partition.
     * This identifier is used to track the specific ledger
     * that the partition data belongs to within a storage system.
     */
    private final int ledger;
    /**
     * The epoch number indicating the version or generation of the partition.
     * It helps in identifying the current state or version of the partition metadata.
     */
    private final int epoch;
    /**
     * The server that is currently the leader for the partition.
     */
    private final String leader;
    /**
     * A set of replicas for the partition, indicating the nodes that hold copies
     * of the partition's data. This is essential for ensuring data redundancy and
     * fault tolerance within the distributed system.
     */
    private final Set<String> replicas;
    /**
     * Holds configuration settings specific to the topic.
     */
    private final TopicConfig topicConfig;
    /**
     * The schema version of the partition. This version number is used
     * to identify and manage different versions of the partition's schema.
     */
    private final int version;
    /**
     * Unique identifier for the topic associated with this partition.
     */
    private int topicId;

    /**
     * Constructs a PartitionInfo instance containing the metadata of a partition within a topic.
     *
     * @param topic       The name of the topic this partition belongs to.
     * @param topicId     The unique identifier of the topic.
     * @param partition   The partition number within the topic.
     * @param ledger      The ledger ID associated with this partition.
     * @param epoch       The current epoch of this partition.
     * @param leader      The leader node managing this partition.
     * @param replicas    A set of replica nodes for this partition.
     * @param topicConfig The configuration details of the topic.
     * @param version     The version of this partition metadata information.
     */
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

    /**
     * Retrieves the topic associated with this PartitionInfo.
     *
     * @return the topic name
     */
    public String getTopic() {
        return topic;
    }

    /**
     * Retrieves the partition number.
     *
     * @return the partition number of this partition.
     */
    public int getPartition() {
        return partition;
    }

    /**
     * Retrieves the ledger associated with this partition.
     *
     * @return the ledger identifier.
     */
    public int getLedger() {
        return ledger;
    }

    /**
     * Retrieves the epoch of the partition.
     *
     * @return the epoch of the partition.
     */
    public int getEpoch() {
        return epoch;
    }

    /**
     * Retrieves the leader for the partition.
     *
     * @return the leader of the partition as a String
     */
    public String getLeader() {
        return leader;
    }

    /**
     * Returns the set of replicas associated with the partition.
     *
     * @return A set of replica identifiers.
     */
    public Set<String> getReplicas() {
        return replicas;
    }

    /**
     * Retrieves the configuration settings of the topic associated with this partition.
     *
     * @return The TopicConfig instance containing the topic's configuration settings.
     */
    public TopicConfig getTopicConfig() {
        return topicConfig;
    }

    /**
     * Returns the version of the partition information.
     *
     * @return the version number.
     */
    public int getVersion() {
        return version;
    }

    /**
     * Retrieves the unique identifier for the current topic.
     *
     * @return the unique identifier (ID) of the topic.
     */
    public int getTopicId() {
        return topicId;
    }

    /**
     * Sets the topic ID for this PartitionInfo.
     *
     * @param topicId the new topic ID to set
     */
    public void setTopicId(int topicId) {
        this.topicId = topicId;
    }

    /**
     * Compares this PartitionInfo to the specified object.
     *
     * @param o the object to be compared for equality with this PartitionInfo.
     * @return true if the specified object is equal to this PartitionInfo, false otherwise.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PartitionInfo that = (PartitionInfo) o;
        return partition == that.partition && ledger == that.ledger && Objects.equals(topic, that.topic);
    }

    /**
     * Computes the hash code for this PartitionInfo instance.
     *
     * @return the hash code based on the topic, partition, and ledger fields.
     */
    @Override
    public int hashCode() {
        return Objects.hash(topic, partition, ledger);
    }

    /**
     * Returns a string representation of the PartitionInfo object, displaying its
     * fields such as topic, topicId, partition, ledger, epoch, leader, replicas,
     * topicConfig, and version.
     *
     * @return a string that represents the PartitionInfo object.
     */
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
