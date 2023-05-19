package org.ostara.remote.processor;


public interface ProcessCommand {
    interface Server {
        byte SEND_MESSAGE = 1;
        byte QUERY_TOPIC_INFOS = 2;
        byte QUERY_CLUSTER_INFOS = 3;
        byte REST_SUBSCRIBE = 4;
        byte ALTER_SUBSCRIBE = 5;
        byte CLEAN_SUBSCRIBE = 6;
        byte CREATE_TOPIC = 7;
        byte DELETE_TOPIC = 8;
    }

    interface Client {
      byte PUSH_MESSAGE = 1;
      byte SERVER_OFFLINE = 2;
      byte TOPIC_INFO_CHANGED = 3;
    }

    interface Failure {
        byte UNKNOWN_EXCEPTION = -1;
        byte TIMEOUT_EXCEPTION = -2;
        byte CONNECT_EXCEPTION = -3;
        byte COMMAND_EXCEPTION = -4;
        byte PROCESS_EXCEPTION = -5;
    }
}
