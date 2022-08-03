package org.shallow.processor;


public interface ProcessCommand {
    interface Server {
        Server ACTIVE = new Server() {
            @Override
            public String get(byte command) {
                return switch (command) {
                    case 1 -> "CREATE_TOPIC";
                    case 2 -> "DELETE_TOPIC";
                    case 3 -> "FETCH_CLUSTER_INFO";
                    case 4 -> "FETCH_TOPIC_INFO";
                    default -> throw new IllegalStateException("Unexpected client command: " + command);
                };
            }
        };

        byte CREATE_TOPIC = 1;
        byte DELETE_TOPIC = 2;
        byte FETCH_CLUSTER_INFO = 3;
        byte FETCH_TOPIC_INFO = 4;
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

    interface NameServer {
        NameServer ACTIVE = new NameServer() {
            @Override
            public String get(byte command) {
                return switch (command) {
                    case 1 -> "REGISTER_NODE";
                    case 2 -> "OFFLINE";
                    case 3 -> "NEW_TOPIC";
                    case 4 -> "DELETE_TOPIC";
                    case 5 -> "QUERY_CLUSTER_NODE";
                    case 6 -> "QUERY_TOPIC_INFO";
                    default -> throw new IllegalStateException("Unexpected server command: " + command);
                };
            }
        };

        byte REGISTER_NODE = 1;
        byte HEART_BEAT = 2;
        byte NEW_TOPIC = 3;
        byte REMOVE_TOPIC = 4;
        byte QUERY_CLUSTER_IFO = 5;
        byte QUERY_TOPIC_INFO = 6;
        default String get(byte command) {
            return null;
        }
    }
}
