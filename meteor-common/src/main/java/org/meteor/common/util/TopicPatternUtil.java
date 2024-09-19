package org.meteor.common.util;

import java.util.regex.Pattern;

/**
 * A utility class for performing validation on topics, partitions, queues, and ledger replicas.
 */
public class TopicPatternUtil {
    /**
     * A regex pattern to validate topic names.
     * This pattern ensures that the topic name consists of alphanumeric characters,
     * underscores, hyphens, and hashes only. The topic name should match the
     * pattern entirely from start to end.
     * <p>
     * The valid characters are:
     * - Alphanumeric (a-z, A-Z, 0-9)
     * - Underscore (_)
     * - Hyphen (-)
     * - Hash (#)
     */
    private static final Pattern TOPIC_PATTERN = Pattern.compile("^[\\w\\-#]+$");

    /**
     * Validates the given topic against certain criteria.
     *
     * @param topic the topic to be validated
     * @throws NullPointerException if the topic is null
     * @throws IllegalStateException if the topic does not match the pattern or exceeds the maximum length of 127 characters
     */
    public static void validateTopic(String topic) {
        if (topic == null) {
            throw new NullPointerException("Topic cannot be empty");
        }

        if (!TOPIC_PATTERN.matcher(topic).matches()) {
            throw new IllegalStateException(String.format("Topic[%s] is invalid", topic));
        }

        if (topic.length() > 127) {
            throw new IllegalStateException(
                    String.format("Topic[%s] is too large, and its the max length is 127 bytes ", topic));
        }
    }

    /**
     * Validates the number of partitions for a topic.
     *
     * @param number the number of partitions specified for the topic.
     *               Should be greater than 0.
     * @throws IllegalArgumentException if the number of partitions is less than or equal to 0.
     */
    public static void validatePartition(int number) {
        if (number <= 0) {
            throw new IllegalArgumentException(String.format("Partition limit[%d] should be > 0", number));
        }
    }

    /**
     * Validates the number of ledger replicas.
     *
     * @param number the number of ledger replicas to validate
     * @throws IllegalArgumentException if the number of replicas is less than 0
     */
    public static void validateLedgerReplica(int number) {
        if (number < 0) {
            throw new IllegalArgumentException(String.format("Ledger replicas limit[%d] should be > 0", number));
        }
    }

    /**
     * Validates the specified queue.
     * The queue should not be null and its length should not exceed 127 characters.
     *
     * @param queue the name of the queue to be validated
     * @throws NullPointerException if the queue is null
     * @throws IllegalStateException if the queue length exceeds 127 characters
     */
    public static void validateQueue(String queue) {
        if (queue == null) {
            throw new NullPointerException("Topic queue cannot be empty");
        }
        if (queue.length() > 127) {
            throw new IllegalStateException(
                    String.format("Queue[%s] is too large, and its the max length is 127 bytes", queue));
        }
    }
}
