package org.meteor.zookeeper;

/**
 * The {@code CorrelationIdConstants} class defines a set of constants used
 * for identifying and managing topics and ledgers in the system.
 * <p>
 * This class contains constants that are used as keys and identifiers
 * within topic and ledger operations, particularly in a Zookeeper-backed
 * environment.
 */
public final class CorrelationIdConstants {
    /**
     * A constant used as a key for the topic ID counter in the system.
     * This counter is typically used to uniquely identify and manage topics,
     * with its value incremented in a Zookeeper-backed environment.
     */
    public static final String TOPIC_ID_COUNTER = "/topic_id_counter";
    /**
     * Constant used to identify the counter for ledger IDs within the system.
     * This counter is typically used in a Zookeeper-backed environment to
     * manage unique ledger identifiers.
     */
    public static final String LEDGER_ID_COUNTER = "/ledger_id_counter";
    /**
     * A constant used to represent the unique identifier of a topic within
     * the system. This identifier is typically used in the context of
     * Zookeeper-backed topic and ledger management operations.
     */
    public static final String TOPIC_ID = "topicId";
    /**
     * Represents the identifier used in the system to specify the replicas of partitions
     * within topics or ledgers.
     * <p>
     * This constant is part of the {@code CorrelationIdConstants} class, which
     * provides a set of constants for managing topics and ledgers, especially
     * in Zookeeper-backed environments.
     */
    public static final String PARTITION_REPLICAS = "partitionReplicas";
}
