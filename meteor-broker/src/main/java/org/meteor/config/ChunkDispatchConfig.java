package org.meteor.config;

import static org.meteor.common.util.ObjectLiteralsTransformUtil.object2Int;
import java.util.Properties;

/**
 * Configuration class for chunk dispatch settings.
 * This class reads and manages chunk dispatch-related properties from a given Properties object.
 */
public class ChunkDispatchConfig {
    /**
     * Configuration key for the load limit of a chunk dispatch entry.
     * Used to control the maximum number of entries loaded in a single chunk dispatch operation.
     */
    private static final String CHUNK_DISPATCH_ENTRY_LOAD_LIMIT = "chunk.dispatch.entry.load.limit";
    /**
     * The maximum number of entries that can be followed during chunk dispatch.
     * This setting is controlled by the property key "chunk.dispatch.entry.follow.limit"
     * and is used to configure the chunk dispatch process.
     */
    private static final String CHUNK_DISPATCH_ENTRY_FOLLOW_LIMIT = "chunk.dispatch.entry.follow.limit";
    /**
     * Specifies the limit for the pursue phase of chunk dispatch entries.
     * This configuration parameter controls the maximum number of chunk entries
     * that can be pursued in one dispatch operation.
     */
    private static final String CHUNK_DISPATCH_ENTRY_PURSUE_LIMIT = "chunk.dispatch.entry.pursue.limit";
    /**
     * Property key to specify the alignment limit for chunk dispatch entries.
     * This is used within the chunk dispatch configuration to determine the maximum number of entries
     * that can be aligned in one dispatch operation.
     */
    private static final String CHUNK_DISPATCH_ENTRY_ALIGN_LIMIT = "chunk.dispatch.entry.align.limit";
    /**
     * Property key for the timeout duration in milliseconds for pursuing chunk dispatch entries.
     * This specifies the maximum period to wait before a chunk dispatch entry is considered as timed-out.
     */
    private static final String CHUNK_DISPATCH_ENTRY_PURSUE_TIMEOUT_MILLISECONDS = "chunk.dispatch.entry.pursue.timeout.milliseconds";
    /**
     * Defines the default byte limit for a chunk dispatch entry.
     * This is used in the {@link ChunkDispatchConfig} class to limit the maximum number of bytes
     * that can be dispatched in a single chunk.
     * The default value is set at 65536 bytes, but can be configured via a {@code Properties} object.
     */
    private static final String CHUNK_DISPATCH_ENTRY_BYTES_LIMIT = "chunk.dispatch.entry.bytes.limit";
    /**
     * Property key for the semaphore limit used during chunk synchronization.
     */
    private static final String CHUNK_SYNC_SEMAPHORE_LIMIT = "chunk.sync.semaphore.limit";
    /**
     * Properties object that holds the chunk dispatch-related settings.
     * The property names for the settings are predefined and expected to be present
     * within the provided Properties object. This variable is used to fetch configuration
     * values such as limits and timeouts for chunk dispatch operations.
     */
    private final Properties prop;

    /**
     * Constructor for ChunkDispatchConfig.
     * <p>
     * Initializes the configuration by reading properties from the given Properties object.
     *
     * @param prop Properties object containing chunk dispatch configuration settings.
     */
    public ChunkDispatchConfig(Properties prop) {
        this.prop = prop;
    }

    /**
     * Retrieves the value for the chunk dispatch synchronization semaphore limit
     * from the properties. If the value is not set, it defaults to 100.
     *
     * @return the chunk dispatch sync semaphore limit as an integer
     */
    public int getChunkDispatchSyncSemaphore() {
        return object2Int(prop.getOrDefault(CHUNK_SYNC_SEMAPHORE_LIMIT, 100));
    }

    /**
     * Retrieves the chunk dispatch entry bytes limit from the configuration properties.
     * If the property is not set, a default value of 65536 is returned.
     *
     * @return the chunk dispatch entry bytes limit
     */
    public int getChunkDispatchEntryBytesLimit() {
        return object2Int(prop.getOrDefault(CHUNK_DISPATCH_ENTRY_BYTES_LIMIT, 65536));
    }

    /**
     * Retrieves the load limit for chunk dispatch entries.
     * The load limit is determined by the property 'chunk.dispatch.entry.load.limit'.
     * If the property is not set, a default value of 50 is used.
     *
     * @return the load limit for chunk dispatch entries
     */
    public int getChunkDispatchEntryLoadLimit() {
        return object2Int(prop.getOrDefault(CHUNK_DISPATCH_ENTRY_LOAD_LIMIT, 50));
    }

    /**
     * Retrieves the value of the chunk dispatch follow limit from the configuration properties.
     * If the property is not set, the default value of 100 is returned.
     *
     * @return the chunk dispatch entry follow limit as an integer
     */
    public int getChunkDispatchEntryFollowLimit() {
        return object2Int(prop.getOrDefault(CHUNK_DISPATCH_ENTRY_FOLLOW_LIMIT, 100));
    }

    /**
     * Retrieves the chunk dispatch entry pursue limit from the configuration properties.
     * If the property is not defined, returns a default value of 500.
     *
     * @return the pursue limit for chunk dispatch entries
     */
    public int getChunkDispatchEntryPursueLimit() {
        return object2Int(prop.getOrDefault(CHUNK_DISPATCH_ENTRY_PURSUE_LIMIT, 500));
    }

    /**
     * Retrieves the chunk dispatch entry alignment limit configuration.
     * The value determines the alignment limit for chunk dispatch entries and defaults to 2000 if not set.
     *
     * @return the chunk dispatch entry alignment limit.
     */
    public int getChunkDispatchEntryAlignLimit() {
        return object2Int(prop.getOrDefault(CHUNK_DISPATCH_ENTRY_ALIGN_LIMIT, 2000));
    }

    /**
     * Retrieves the timeout value in milliseconds for pursuing a chunk dispatch entry.
     *
     * @return the chunk dispatch entry pursue timeout in milliseconds.
     */
    public int getChunkDispatchEntryPursueTimeoutMilliseconds() {
        return object2Int(prop.getOrDefault(CHUNK_DISPATCH_ENTRY_PURSUE_TIMEOUT_MILLISECONDS, 10000));
    }
}
