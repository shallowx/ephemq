package org.meteor.config;

import static org.meteor.common.util.ObjectLiteralsTransformUtil.object2Int;
import java.util.Properties;

/**
 * The DefaultDispatchConfig class provides configuration settings for various dispatch operations.
 * It extracts the configuration properties from a Properties object and provides default values if the properties are not set.
 */
public class DefaultDispatchConfig {
    /**
     * Configuration key for the maximum number of loads allowed in a dispatch entry.
     * This value is retrieved from the configuration properties and determines
     * the load limit applied to each dispatch entry. If not specified in the properties,
     * a default value of 50 is used.
     */
    private static final String DISPATCH_ENTRY_LOAD_LIMIT = "dispatch.entry.load.limit";
    /**
     * Configuration key representing the limit for the number of entries to follow in a dispatch operation.
     */
    private static final String DISPATCH_ENTRY_FOLLOW_LIMIT = "dispatch.entry.follow.limit";
    /**
     * Configuration key for the pursue limit in dispatch entries.
     * This key is used to retrieve the maximum number of pursue operations allowed
     * from a Properties object. The default value is 500 if not specified.
     */
    private static final String DISPATCH_ENTRY_PURSUE_LIMIT = "dispatch.entry.pursue.limit";
    /**
     * Configuration property key for defining the dispatch entry alignment limit.
     * This key is used to retrieve the alignment limit from the configuration properties.
     * If this property is not defined, the default value of 2000 is used.
     */
    private static final String DISPATCH_ENTRY_ALIGN_LIMIT = "dispatch.entry.align.limit";
    /**
     * Specifies the timeout, in milliseconds, for pursuing a dispatch entry.
     * Utilized within the DefaultDispatchConfig class to configure the time
     * allowed for a dispatch pursuit operation before it times out.
     */
    private static final String DISPATCH_ENTRY_PURSUE_TIMEOUT_MILLISECONDS = "dispatch.entry.pursue.timeout.milliseconds";
    /**
     * A Properties object that holds configuration settings for dispatch operations.
     * This variable is used to retrieve configuration values such as load limits, follow limits,
     * pursue limits, align limits, and pursue timeouts, providing defaults if the properties are not set.
     */
    private final Properties prop;

    /**
     * Constructs a DefaultDispatchConfig instance with the specified properties.
     *
     * @param prop the properties object containing configuration settings
     */
    public DefaultDispatchConfig(Properties prop) {
        this.prop = prop;
    }

    /**
     * Retrieves the dispatch entry load limit from the configuration properties.
     * If the property is not defined, a default value of 50 is returned.
     *
     * @return the dispatch entry load limit as an integer, which is either the configured value or the default value of 50.
     */
    public int getDispatchEntryLoadLimit() {
        return object2Int(prop.getOrDefault(DISPATCH_ENTRY_LOAD_LIMIT, 50));
    }

    /**
     * Retrieves the dispatch entry follow limit from the configuration properties.
     * If the property is not set, it returns a default value of 100.
     *
     * @return the dispatch entry follow limit as an integer.
     */
    public int getDispatchEntryFollowLimit() {
        return object2Int(prop.getOrDefault(DISPATCH_ENTRY_FOLLOW_LIMIT, 100));
    }

    /**
     * Retrieves the dispatch entry pursue limit from the configuration properties.
     * If the property is not set, it returns the default value of 500.
     *
     * @return the dispatch entry pursue limit
     */
    public int getDispatchEntryPursueLimit() {
        return object2Int(prop.getOrDefault(DISPATCH_ENTRY_PURSUE_LIMIT, 500));
    }

    /**
     * Retrieves the dispatch entry alignment limit from the configuration properties.
     * If the property is not set, a default value of 2000 is returned.
     *
     * @return the dispatch entry alignment limit as an integer.
     */
    public int getDispatchEntryAlignLimit() {
        return object2Int(prop.getOrDefault(DISPATCH_ENTRY_ALIGN_LIMIT, 2000));
    }

    /**
     * Retrieves the dispatch entry pursue timeout in milliseconds.
     * This value represents the maximum duration to wait for pursuing a dispatch entry.
     * If the property is not set, a default value of 10000 milliseconds is returned.
     *
     * @return the dispatch entry pursue timeout in milliseconds.
     */
    public int getDispatchEntryPursueTimeoutMilliseconds() {
        return object2Int(prop.getOrDefault(DISPATCH_ENTRY_PURSUE_TIMEOUT_MILLISECONDS, 10000));
    }
}
