package org.leopard.remote.processor;


public interface ProcessCommand {
    interface Server {
        byte CREATE_TOPIC = 1;
        byte DELETE_TOPIC = 2;
        byte FETCH_CLUSTER_RECORD = 3;
        byte FETCH_TOPIC_RECORD = 4;
        byte SEND_MESSAGE = 5;
        byte SUBSCRIBE = 6;
        byte PULL_MESSAGE = 7;
        byte CLEAN_SUBSCRIBE = 8;

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

    interface Nameserver {
        byte NEW_TOPIC = 1;
        byte REGISTER_NODE = 2;
        byte QUERY_TOPIC = 3;
        byte QUERY_NODE = 4;
        byte HEARTBEAT = 5;
        byte UN_REGISTER_NODE = 6;
    }
}
