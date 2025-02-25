package org.meteor.support;

/**
 * This class defines a set of constants representing various paths used for broker and topic
 * management within a ZooKeeper-based distributed system. These constants help form structured
 * URLs which are uniformly used throughout the application to interact with different parts
 * of the ZooKeeper hierarchy.
 */
@SuppressWarnings("all")
public final class PathConstants {
    /**
     * Path constant for accessing the controller.
     */
    public static final String CONTROLLER = "/controller";
    /**
     * Represents the base path for brokers.
     * This constant is used as a prefix to construct
     * more specific broker-related paths within the system.
     */
    public static final String BROKERS = "/brokers";
    /**
     * Represents the path for accessing topics under brokers in a distributed system.
     * It combines the base path for brokers with the relative path for topics.
     * It is used to organize and retrieve information related to broker topics.
     */
    public static final String BROKERS_TOPICS = STR."\{BROKERS}/topics";
    /**
     * Represents the path template for accessing a specific broker topic.
     * This constant is a format string where the topic name should be inserted.
     * It combines the base path for broker topics with a placeholder for the topic name.
     */
    public static final String BROKER_TOPIC = STR."\{BROKERS_TOPICS}/%s";
    /**
     * Path constant for identifying a specific broker topic's ID.
     * The path is constructed using the base broker topic path and appending "/id".
     */
    public static final String BROKER_TOPIC_ID = STR."\{BROKER_TOPIC}/id";
    /**
     * Represents the path used to access the partitions of a specific broker topic.
     * It is constructed by appending "/partitions" to the BROKER_TOPIC path.
     */
    public static final String BROKER_TOPIC_PARTITIONS = STR."\{BROKER_TOPIC}/partitions";
    /**
     * Represents the path to a specific broker topic partition. The syntax of this constant
     * follows the format: "/brokers/topics/{topicName}/partitions/{partitionId}" where
     * the placeholder %d needs to be replaced with the specific partition ID.
     */
    public static final String BROKER_TOPIC_PARTITION = STR."\{BROKER_TOPIC_PARTITIONS}/%d";
    /**
     * A constant representing the path to broker IDs.
     * This is used to fetch or manipulate broker IDs within the system.
     */
    public static final String BROKERS_IDS = "/brokers/ids";
    /**
     * A constant representing the path for a specific broker ID.
     * This path is used in broker-related operations where a broker's unique identifier is required.
     * The placeholder "%s" should be replaced with the actual broker ID during runtime.
     */
    public static final String BROKERS_ID = STR."\{BROKERS_IDS}/%s";
}
