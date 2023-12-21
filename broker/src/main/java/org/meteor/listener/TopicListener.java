package org.meteor.listener;

import org.meteor.common.message.TopicAssignment;
import org.meteor.common.message.TopicPartition;

public interface TopicListener {
    void onPartitionInit(TopicPartition topicPartition, int ledger);

    void onPartitionDestroy(TopicPartition topicPartition, int ledger);

    void onPartitionGetLeader(TopicPartition topicPartition);

    void onPartitionLostLeader(TopicPartition topicPartition);

    void onTopicCreated(String topic);

    void onTopicDeleted(String topic);

    void onPartitionChanged(TopicPartition topicPartition, TopicAssignment oldAssigment, TopicAssignment newAssigment);
}
