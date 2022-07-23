package org.shallow.processor;


public interface ProcessCommand {
    interface Server {

        Server ACTIVE = new Server() {
            @Override
            public String obtain(byte command) {
                return switch (command) {
                    case 1 -> "CREATE_TOPIC";
                    case 2 -> "DELETE_TOPIC";
                    case 3 -> "UPDATE_TOPIC";
                    case 4 -> "FETCH_CLUSTER_INFO";
                    case 5 -> "FETCH_TOPIC_INFO";
                    default -> throw new IllegalStateException("Unexpected client command: " + command);
                };
            }
        };

        byte CREATE_TOPIC = 1;
        byte DELETE_TOPIC = 2;
        byte UPDATE_TOPIC = 3;
        byte FETCH_CLUSTER_INFO = 4;
        byte FETCH_TOPIC_INFO = 5;

       default String obtain(byte command) {
           return null;
       }
    }

    interface Client {

        Client ACTIVE = new Client() {
            @Override
            public String obtain(byte command) {
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

       default String obtain(byte command) {
           return null;
       }
    }
}
