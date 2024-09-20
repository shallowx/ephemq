package org.meteor.client.core;

/**
 * A utility class that contains constant values for message-related keys and identifiers.
 * This class provides commonly used constant strings that can be referenced throughout
 * the application to ensure consistency and avoid hardcoding values.
 */
public class MessageConstants {
    /**
     * Represents the name of the pending task for client-side Netty operations.
     * This constant is used to identify and manage tasks that are pending execution
     * within the client's Netty framework.
     */
    public static final String CLIENT_NETTY_PENDING_TASK_NAME = "client_netty_pending_task";
    /**
     * A constant string used as a key or identifier for channel semaphore operations in the application.
     * This constant helps maintain consistency and avoid hardcoding when referencing channel semaphore mechanisms.
     */
    public static final String CHANNEL_SEMAPHORE = "channel_semaphore";
}
