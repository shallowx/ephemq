package org.meteor.remote.processor;


public interface ProcessCommand {
    interface Server {
        int SEND_MESSAGE = 1;
        int QUERY_TOPIC_INFOS = 2;
        int QUERY_CLUSTER_INFOS = 3;
        int REST_SUBSCRIBE = 4;
        int ALTER_SUBSCRIBE = 5;
        int CLEAN_SUBSCRIBE = 6;
        int CREATE_TOPIC = 7;
        int DELETE_TOPIC = 8;
        int SYNC_LEDGER = 9;
        int CANCEL_SYNC_LEDGER = 10;
        int CALCULATE_PARTITIONS = 11;

        int MIGRATE_LEDGER = 12;
    }

    interface Client {
        int PUSH_MESSAGE = 1;
        int SERVER_OFFLINE = 2;
        int TOPIC_CHANGED = 3;
        int SYNC_MESSAGE = 4;
    }

    interface Failure {
        int UNKNOWN_EXCEPTION = -1;
        int TIMEOUT_EXCEPTION = -2;
        int CONNECT_EXCEPTION = -3;
        int COMMAND_EXCEPTION = -4;
        int PROCESS_EXCEPTION = -5;
    }
}
