package org.ephemq.remote.invoke;


/**
 * The Command interface defines constants representing various command types for server,
 * client, and failure operations in the messaging system.
 */
public interface Command {
    interface Server {
        /**
         * Identifier for the SEND_MESSAGE operation in the Server interface.
         * Used to denote the action of sending a message.
         */
        int SEND_MESSAGE = 1;
        /**
         * Represents the operation code for querying topic information in the server.
         */
        int QUERY_TOPIC_INFOS = 2;
        /**
         * Query identifier for requesting cluster information.
         * This constant is used to specify the type of query operation related to cluster info retrieval
         * when interacting with the server.
         */
        int QUERY_CLUSTER_INFOS = 3;
        /**
         * The REST_SUBSCRIBE operation identifier.
         * <p>
         * This constant is used to denote a RESTful subscription operation
         * within the context of a server. The value assigned to this
         * identifier is 4.
         */
        int REST_SUBSCRIBE = 4;
        /**
         * Constant representing the action to alter a subscription in the server.
         */
        int ALTER_SUBSCRIBE = 5;
        /**
         * Represents the operation code for a "clean subscribe" action in the server interface.
         * This operation typically involves removing old subscription states and re-registering
         * a fresh subscription with the server.
         */
        int CLEAN_SUBSCRIBE = 6;
        /**
         * Operation code for creating a new topic.
         */
        int CREATE_TOPIC = 7;
        /**
         * Identifier constant representing the action to delete a topic in the server.
         */
        int DELETE_TOPIC = 8;
        /**
         * Represents the operation code for synchronizing the ledger.
         * This operation ensures that the ledger is up-to-date and consistent
         * across different nodes in the system.
         */
        int SYNC_LEDGER = 9;
        /**
         * Represents the constant value to cancel the synchronization of a ledger.
         * This value is used to indicate an operation for cancelling the synchronization process within the server.
         */
        int CANCEL_SYNC_LEDGER = 10;
        /**
         * Constant representing the operation code to calculate partitions.
         * Typically used for server operations that require partition information
         * calculation, indexed numerically within the system.
         */
        int CALCULATE_PARTITIONS = 11;
        /**
         * Represents the operation code for migrating a ledger within the server interface.
         * This constant is used to identify the migrate ledger operation in communication protocols.
         */
        int MIGRATE_LEDGER = 12;
    }

    interface Client {
        /**
         * Represents a constant integer value used to identify a push message
         * event in the client-server communication protocol.
         */
        int PUSH_MESSAGE = 1;
        /**
         * Constant indicating the server is offline.
         */
        int SERVER_OFFLINE = 2;
        /**
         * Indicates that the topic of discussion has changed.
         * This constant can be used to handle cases where an event
         * regarding the change of topic needs to be identified and processed.
         */
        int TOPIC_CHANGED = 3;
        /**
         * Represents a synchronization message used within the `Client` interface.
         * This constant has a value of 4 and is utilized to indicate synchronization events.
         */
        int SYNC_MESSAGE = 4;
    }

    interface Failure {
        /**
         * Represents an unknown exception error code.
         * This error code is used when the exception type cannot be determined.
         */
        int UNKNOWN_EXCEPTION = -1;
        /**
         * Error code indicating that the operation failed due to a timeout.
         * This typically happens when an operation takes longer than the
         * allocated time limit to complete, resulting in an automatic failure.
         */
        int TIMEOUT_EXCEPTION = -2;
        /**
         * This constant represents an error code for connection exceptions.
         * It is used to indicate that an error occurred while attempting to
         * establish a connection.
         */
        int CONNECT_EXCEPTION = -3;
        /**
         * Represents an error code indicating that an exception occurred
         * while executing a command. This could be used to identify issues
         * specifically related to command execution in various contexts
         * where commands are processed.
         */
        int COMMAND_EXCEPTION = -4;
        /**
         * Represents a process-related error condition.
         * <p>
         * This constant is used within the context of failure handling to denote
         * exceptions that occur specifically during a processing operation.
         * A value of -5 indicates that the exception is related to the process
         * execution.
         */
        int PROCESS_EXCEPTION = -5;
    }
}
