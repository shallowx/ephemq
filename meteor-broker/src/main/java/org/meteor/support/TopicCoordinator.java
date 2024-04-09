package org.meteor.support;

import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.meteor.common.message.PartitionInfo;
import org.meteor.common.message.TopicConfig;
import org.meteor.common.message.TopicPartition;
import org.meteor.listener.TopicListener;

public interface TopicCoordinator {
    void start() throws Exception;

    Map<String, Object> createTopic(String topic, int partitions, int replicas, TopicConfig config) throws Exception;

    void deleteTopic(String topic) throws Exception;

    void initPartition(TopicPartition topicPartition, int ledgerId, int epoch, TopicConfig topicConfig) throws Exception;

    boolean hasLeadership(int ledger);

    void retirePartition(TopicPartition topicPartition) throws Exception;

    void handoverPartition(String heir, TopicPartition topicPartition) throws Exception;

    void takeoverPartition(TopicPartition topicPartition) throws Exception;

    @Nullable
    Set<String> getAllTopics() throws Exception;

    void destroyTopicPartition(TopicPartition topicPartition, int ledgerId) throws Exception;

    @Nullable
    PartitionInfo getPartitionInfo(TopicPartition topicPartition) throws Exception;

    @Nullable
    Set<PartitionInfo> getTopicInfo(String topic);

    @Nullable
    List<TopicListener> getTopicListener();

    void addTopicListener(TopicListener listener);

    ParticipantCoordinator getParticipantCoordinator();

    Map<String, Integer> calculatePartitions() throws Exception;

    void shutdown() throws Exception;
}
