package org.meteor.internal;

public interface PathConstants {
    String CONTROLLER = "/controller";
    String BROKERS = "/brokers";
    String BROKERS_TOPICS = BROKERS + "/topics";
    String BROKER_TOPIC = BROKERS_TOPICS + "/%s";
    String BROKER_TOPIC_ID = BROKER_TOPIC + "/id";
    String BROKER_TOPIC_PARTITIONS = BROKER_TOPIC + "/partitions";
    String BROKER_TOPIC_PARTITION = BROKER_TOPIC_PARTITIONS + "/%d";

    String BROKERS_IDS = "/brokers/ids";
    String BROKERS_ID = BROKERS_IDS + "/%s";
}
