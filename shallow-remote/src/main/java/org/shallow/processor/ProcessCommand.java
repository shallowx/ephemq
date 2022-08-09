package org.shallow.processor;


public interface ProcessCommand {
    interface Server {
        Server ACTIVE = new Server() {
            @Override
            public String get(byte command) {
                return switch (command) {
                    case 1 -> "CREATE_TOPIC";
                    case 2 -> "DELETE_TOPIC";
                    case 3 -> "FETCH_CLUSTER_RECORD";
                    case 4 -> "FETCH_TOPIC_RECORD";
                    case 5 -> "QUORUM_VOTE";
                    case 6 -> "HEARTBEAT";
                    default -> throw new IllegalStateException("Unexpected client command: " + command);
                };
            }
        };

        byte CREATE_TOPIC = 1;
        byte DELETE_TOPIC = 2;
        byte FETCH_CLUSTER_RECORD = 3;
        byte FETCH_TOPIC_RECORD = 4;
        byte QUORUM_VOTE = 5;
        byte HEARTBEAT = 6;
       default String get(byte command) {
           return null;
       }
    }

    interface Client {
        Client ACTIVE = new Client() {
            @Override
            public String get(byte command) {
                return switch (command) {
                    case 1 -> "RECEIVE_MESSAGE";
                    case 2 -> "TOPIC_CHANGED";
                    case 3 -> "CLUSTER_CHANGED";
                    default -> throw new IllegalStateException("Unexpected server command: " + command);
                };
            }
        };

        byte RECEIVE_MESSAGE = 1;
        byte TOPIC_CHANGED = 2;
        byte CLUSTER_CHANGED = 3;
       default String get(byte command) {
           return null;
       }
    }
}
