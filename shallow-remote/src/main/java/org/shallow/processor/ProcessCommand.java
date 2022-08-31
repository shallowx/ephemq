package org.shallow.processor;


public interface ProcessCommand {
    interface Server {
        byte CREATE_TOPIC = 1;
        byte DELETE_TOPIC = 2;
        byte FETCH_CLUSTER_RECORD = 3;
        byte FETCH_TOPIC_RECORD = 4;
        byte QUORUM_VOTE = 5;
        byte HEARTBEAT = 6;
        byte PREPARE_COMMIT = 7;
        byte POST_COMMIT = 8;
        byte REGISTER_NODE = 9;
        byte SEND_MESSAGE = 10;
        byte SUBSCRIBE = 11;
        byte PULL_MESSAGE = 12;
        byte CLEAN_SUBSCRIBE = 13;
    }

    interface Client {
        Client ACTIVE = new Client() {
            @Override
            public String get(byte command) {
                return switch (command) {
                    case 1 -> "HANDLE_MESSAGE";
                    case 2 -> "TOPIC_CHANGED";
                    case 3 -> "CLUSTER_CHANGED";
                    default -> throw new IllegalStateException("Unexpected server command: " + command);
                };
            }
        };

        byte HANDLE_MESSAGE = 1;
        byte TOPIC_CHANGED = 2;
        byte CLUSTER_CHANGED = 3;
       default String get(byte command) {
           return null;
       }
    }
}
