package org.meteor.support;

public final class PathConstants {
    public static final String CONTROLLER = "/controller";
    public static final String BROKERS = "/brokers";
    public static final String BROKERS_TOPICS = BROKERS + "/topics";
    public static final String BROKER_TOPIC = BROKERS_TOPICS + "/%s";
    public static final String BROKER_TOPIC_ID = BROKER_TOPIC + "/id";
    public static final String BROKER_TOPIC_PARTITIONS = BROKER_TOPIC + "/partitions";
    public static final String BROKER_TOPIC_PARTITION = BROKER_TOPIC_PARTITIONS + "/%d";
    public static final String BROKERS_IDS = "/brokers/ids";
    public static final String BROKERS_ID = BROKERS_IDS + "/%s";
}
