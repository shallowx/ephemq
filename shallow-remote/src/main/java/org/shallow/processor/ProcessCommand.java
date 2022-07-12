package org.shallow.processor;

public interface ProcessCommand {
    interface Server {
        byte CREATE_TOPIC = 1;
        byte DELETE_TOPIC = 2;
        byte UPDATE_TOPIC = 3;
        byte FETCH_CLUSTER_INFO = 4;
        byte FETCH_TOPIC_INFO = 5;
    }
}
