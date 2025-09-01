package org.ephemq.listener;

import org.ephemq.common.message.TopicAssignment;
import org.ephemq.common.message.TopicPartition;

/**
 * Interface for listening to various events related to topic partitions.
 */
public interface TopicListener {
    /**
     * This method is called when a partition is initialized.
     *
     * @param topicPartition the topic and partition identifier for the initialized partition
     * @param ledger         the ledger identifier associated with the initialized partition
     */
    void onPartitionInit(TopicPartition topicPartition, int ledger);

    /**
     * Called when a partition is destroyed.
     *
     * @param topicPartition The partition which is being destroyed.
     * @param ledger The ledger associated with the partition being destroyed.
     */
    void onPartitionDestroy(TopicPartition topicPartition, int ledger);

    /**
     * Event triggered when a leader is assigned for a given topic partition.
     *
     * @param topicPartition the specific topic partition that has been assigned a leader
     */
    void onPartitionGetLeader(TopicPartition topicPartition);

    /**
     * Triggered when a leader is lost for a particular partition.
     *
     * @param topicPartition Represents the topic and partition for which the leadership was lost.
     */
    void onPartitionLostLeader(TopicPartition topicPartition);

    /**
     * Callback method invoked when a new topic is created.
     *
     * @param topic The name of the newly created topic.
     */
    void onTopicCreated(String topic);

    /**
     * Called when a topic is deleted.
     *
     * @param topic The name of the topic that has been deleted.
     */
    void onTopicDeleted(String topic);

    /**
     * This method is called when there is a change in the partition assignment of a topic.
     *
     * @param topicPartition The topic partition whose assignment has changed.
     * @param oldAssigment The previous assignment of the topic partition.
     * @param newAssigment The new assignment of the topic partition.
     */
    void onPartitionChanged(TopicPartition topicPartition, TopicAssignment oldAssigment, TopicAssignment newAssigment);
}
