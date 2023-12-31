package org.meteor.client.util;

import java.util.regex.Pattern;

public class TopicPatternUtil {
    private static final Pattern TOPIC_PATTERN = Pattern.compile("^[\\w\\-#]+$");
    public static void validateTopic(String topic) {
        if (topic == null) {
            throw new IllegalArgumentException("Topic not found");
        }

        if (!TOPIC_PATTERN.matcher(topic).matches()) {
            throw new IllegalStateException(String.format("Topic[%s] is invalid", topic));
        }
    }

    public static void validatePartition(int number) {
        if (number <= 0) {
            throw new IllegalArgumentException(String.format("Partition[%d] limit should be > 0", number));
        }
    }

    public static void validateLedgerReplica(int number) {
        if (number <= 0) {
            throw new IllegalArgumentException(String.format("Ledger replicas[%d] limit should be > 0", number));
        }
    }

    public static void validateQueue(String queue) {
        if (queue == null) {
            throw new IllegalArgumentException("Topic queue not found");
        }
    }
}
