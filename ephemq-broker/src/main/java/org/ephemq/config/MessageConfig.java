package org.ephemq.config;

import io.netty.util.NettyRuntime;

import java.util.Properties;

import static org.ephemq.common.util.ObjectLiteralsTransformUtil.object2Int;

/**
 * The MessageConfig class encapsulates the configuration properties for
 * message handling, including limits for message sync, storage, and
 * dispatch threads.
 */
public class MessageConfig {
    /**
     * Constant that specifies the property key used to configure the limit on the number
     * of threads allocated for message synchronization. This key is used to retrieve the
     * value from the properties object passed to the {@link MessageConfig} class.
     * The default value, if not specified, is the number of available processors.
     */
    private static final String MESSAGE_SYNC_THREAD_LIMIT = "message.sync.thread.limit";
    /**
     * Configuration key for the limit on the number of threads allocated
     * for message storage operations.
     */
    private static final String MESSAGE_STORAGE_THREAD_LIMIT = "message.storage.thread.limit";
    /**
     * Constant representing the configuration property key for the limit of
     * dispatch threads used in message handling.
     */
    private static final String MESSAGE_DISPATCH_THREAD_LIMIT = "message.dispatch.thread.limit";
    /**
     * A Properties object that holds the configuration settings
     * for message handling, including sync, storage, and dispatch
     * thread limits.
     */
    private final Properties prop;

    /**
     * Constructs a MessageConfig instance with the given properties.
     *
     * @param prop the properties object containing configuration settings related to message handling
     */
    public MessageConfig(Properties prop) {
        this.prop = prop;
    }

    /**
     * Fetches the limit on the number of threads that can be used for message synchronization.
     *
     * @return the maximum number of threads allowed for message synchronization. If not explicitly set,
     * this defaults to the number of available processors as determined by the runtime.
     */
    public int getMessageSyncThreadLimit() {
        return object2Int(prop.getOrDefault(MESSAGE_SYNC_THREAD_LIMIT, NettyRuntime.availableProcessors()));
    }

    /**
     * Retrieves the limit for the number of threads allocated for message storage operations.
     * The limit is defined by the property {@code message.storage.thread.limit}, if set,
     * otherwise defaults to the number of available processors.
     *
     * @return the maximum number of threads for message storage operations.
     */
    public int getMessageStorageThreadLimit() {
        return object2Int(prop.getOrDefault(MESSAGE_STORAGE_THREAD_LIMIT, NettyRuntime.availableProcessors()));
    }

    /**
     * Retrieves the limit on the number of threads that can be used for
     * dispatching messages. If the property is not set, it defaults
     * to the number of available processors.
     *
     * @return the message dispatch thread limit as an integer.
     */
    public int getMessageDispatchThreadLimit() {
        return object2Int(prop.getOrDefault(MESSAGE_DISPATCH_THREAD_LIMIT, NettyRuntime.availableProcessors()));
    }
}
