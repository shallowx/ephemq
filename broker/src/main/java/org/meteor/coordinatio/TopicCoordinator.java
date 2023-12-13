package org.meteor.coordinatio;

import org.meteor.common.PartitionInfo;
import org.meteor.common.TopicConfig;
import org.meteor.common.TopicPartition;
import org.meteor.listener.TopicListener;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface TopicCoordinator {

    void start() throws Exception;

    Map<String, Object> createTopic(String topic, int partitions, int replicas, TopicConfig config) throws Exception;

    void deleteTopic(String topic) throws Exception;

    void initPartition(TopicPartition topicPartition, int ledgerId, int epoch, TopicConfig topicConfig) throws Exception;

    boolean hasLeadership(int ledger);

    void retirePartition(TopicPartition topicPartition) throws Exception;

    void handoverPartition(String heir, TopicPartition topicPartition) throws Exception;

    void takeoverPartition(TopicPartition topicPartition) throws Exception;

    Set<String> getAllTopics() throws Exception;

    void destroyTopicPartition(TopicPartition topicPartition, int ledgerId) throws Exception;

    PartitionInfo getPartitionInfo(TopicPartition topicPartition) throws Exception;

    void shutdown() throws Exception;

    Set<PartitionInfo> getTopicInfo(String topic);

    List<TopicListener> getTopicListener();

    void addTopicListener(TopicListener listener);

    ParticipantCoordinator getReplicaManager();

    Map<String, Integer> calculatePartitions() throws Exception;

}
