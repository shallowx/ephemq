package org.ephemq.zookeeper;

import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.ephemq.common.message.PartitionInfo;
import org.ephemq.common.message.TopicConfig;
import org.ephemq.common.message.TopicPartition;
import org.ephemq.ledger.ParticipantSupport;
import org.ephemq.listener.TopicListener;

/**
 * Interface TopicHandleSupport defines methods for managing topics and their partitions.
 */
public interface TopicHandleSupport {
    /**
     * Starts the necessary components for handling topics and their partitions.
     *
     * @throws Exception if an error occurs during the startup process.
     */
    void start() throws Exception;

    /**
     * Creates a new topic with the specified configurations.
     *
     * @param topic      The name of the topic to be created.
     * @param partitions The number of partitions for the topic.
     * @param replicas   The number of replicas for each partition.
     * @param config     The configuration settings for the topic.
     * @return A map containing the details of the created topic.
     * @throws Exception If an error occurs during topic creation.
     */
    Map<String, Object> createTopic(String topic, int partitions, int replicas, TopicConfig config) throws Exception;
    /**
     * Deletes the specified topic.
     *
     * @param topic the name of the topic to be deleted
     * @throws Exception if there is an error while deleting the topic
     */
    void deleteTopic(String topic) throws Exception;
    /**
     * Initializes a partition within a topic with the given ledger ID, epoch, and configuration.
     *
     * @param topicPartition the topic and partition to initialize.
     * @param ledgerId       the ID of the ledger associated with the partition.
     * @param epoch          the epoch number for the partition.
     * @param topicConfig    configuration parameters for the topic.
     * @throws Exception if an error occurs during the partition initialization.
     */
    void initPartition(TopicPartition topicPartition, int ledgerId, int epoch, TopicConfig topicConfig) throws Exception;
    /**
     * Checks if the current instance has leadership over the specified ledger.
     *
     * @param ledger the unique identifier of the ledger to check leadership for
     * @return true if the current instance has leadership over the provided ledger; false otherwise
     */
    boolean hasLeadership(int ledger);
    /**
     * Retires a specified partition of a topic. This operation marks the partition as retired,
     * transitioning it out of active state and ensuring its data is no longer accessible for
     * new operations.
     *
     * @param topicPartition the {@link TopicPartition} representing the topic and partition to retire
     * @throws Exception if the retirement process encounters an issue
     */
    void retirePartition(TopicPartition topicPartition) throws Exception;
    /**
     * Transfers the leadership and updates replica information for the specified partition to a new leader.
     *
     * @param heir The new leader to whom the leadership of the partition will be handed over.
     * @param topicPartition The partition for which the leadership will be transferred.
     * @throws Exception If an error occurs during the handover process.
     */
    void handoverPartition(String heir, TopicPartition topicPartition) throws Exception;
    /**
     * Takeover an existing partition identified by the TopicPartition parameter. This method is used to assume control
     * over a specific partition, ensuring the necessary initialization and configuration updates are performed.
     *
     * @param topicPartition the TopicPartition object representing the topic and partition number to be taken over.
     * @throws Exception if any error occurs during the takeover process, including issues with accessing the
     *         data store or updating partition information.
     */
    void takeoverPartition(TopicPartition topicPartition) throws Exception;
    /**
     * Retrieves all available topic names.
     *
     * @return a set of topic names, or null if no topics are available
     * @throws Exception if any error occurs while fetching the topics
     */
    @Nullable
    Set<String> getAllTopics() throws Exception;
    /**
     * Destroys the specified partition of a topic.
     *
     * @param topicPartition the partition of the topic to be destroyed. It consists of the topic name and partition number.
     * @param ledgerId the identifier of the ledger associated with the topic partition to be destroyed.
     * @throws Exception if an error occurs during the destruction of the topic partition.
     */
    void destroyTopicPartition(TopicPartition topicPartition, int ledgerId) throws Exception;
    /**
     * Retrieves the partition information for the given topic partition.
     *
     * @param topicPartition The topic partition for which to get the partition information.
     * @return The partition information for the given topic partition, or null if not available.
     * @throws Exception If an error occurs while retrieving the partition information.
     */
    @Nullable
    PartitionInfo getPartitionInfo(TopicPartition topicPartition) throws Exception;
    /**
     * Retrieves the partition information for a given topic.
     *
     * @param topic the name of the topic whose partition information is to be retrieved
     * @return a set of PartitionInfo objects containing the details of the partitions for the given topic,
     *         or null if the topic does not exist
     */
    @Nullable
    Set<PartitionInfo> getTopicInfo(String topic);

    /**
     * Retrieves the list of TopicListener instances that are currently registered.
     * TopicListeners are notified of various events related to topic partitions such as
     * partition initialization, partition destruction, assignment changes, leader assignment, etc.
     *
     * @return a non-null list of TopicListener instances.
     */
    @Nonnull
    List<TopicListener> getTopicListener();
    /**
     * Adds a listener to receive various events related to topic partitions.
     *
     * @param listener the TopicListener instance to be added
     */
    void addTopicListener(TopicListener listener);

    /**
     * Retrieves the support operations for participants.
     *
     * @return an instance of ParticipantSupport containing participant-related operations.
     */
    ParticipantSupport getParticipantSuooprt();
    /**
     * Calculates the partition mapping in the system.
     *
     * @return A Map where the key is the topic name and the value is the number of partitions.
     * @throws Exception If an error occurs during the calculation.
     */
    Map<String, Integer> calculatePartitions() throws Exception;
    /**
     * Shuts down the various components or services managed by the implementing class.
     * This ensures that all resources are properly cleaned up and terminated.
     *
     * @throws Exception if an error occurs during the shutdown process of any component or service.
     */
    void shutdown() throws Exception;
}
