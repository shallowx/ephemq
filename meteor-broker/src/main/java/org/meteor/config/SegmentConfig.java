package org.meteor.config;

import java.util.Properties;

import static org.meteor.common.util.ObjectLiteralsTransformUtil.object2Int;

/**
 * SegmentConfig manages the configuration settings related to segments.
 * It allows retrieval of segment rolling size, segment retain limit,
 * and segment retention time in milliseconds from a provided Properties object.
 */
public class SegmentConfig {
    /**
     * The configuration key for specifying the segment rolling size.
     * This determines the threshold size, in bytes, at which a segment will roll over and be archived or removed.
     */
    private static final String SEGMENT_ROLLING_SIZE = "segment.rolling.size";
    /**
     * The configuration property key for the limit on the number of segments to retain.
     * It is used to specify the maximum number of segments that should be kept.
     */
    private static final String SEGMENT_RETAIN_LIMIT = "segment.retain.limit";
    /**
     * Property key for configuring the duration in milliseconds for which
     * a segment is retained before being eligible for deletion.
     */
    private static final String SEGMENT_RETAIN_TIME_MILLISECONDS = "segment.retain.time.milliseconds";

    /**
     * A properties object that holds configuration settings for segments.
     * This variable is used to retrieve configuration values such as
     * segment rolling size, retain limit, and retention time.
     */
    private final Properties prop;

    /**
     * Initializes the SegmentConfig with the specified properties.
     *
     * @param prop the Properties object containing configuration settings related to segments.
     */
    public SegmentConfig(Properties prop) {
        this.prop = prop;
    }

    /**
     * Retrieves the segment rolling size from the configuration properties.
     * If the segment rolling size property is not set, a default value of 4194304 is returned.
     *
     * @return the segment rolling size in bytes
     */
    public int getSegmentRollingSize() {
        return object2Int(prop.getOrDefault(SEGMENT_ROLLING_SIZE, 4194304));
    }

    /**
     * Retrieves the segment retain limit from the configuration properties.
     * The segment retain limit determines the maximum number of segments that should be retained.
     * If the property is not set, it defaults to 3.
     *
     * @return the segment retain limit as an integer.
     */
    public int getSegmentRetainLimit() {
        return object2Int(prop.getOrDefault(SEGMENT_RETAIN_LIMIT, 3));
    }

    /**
     * Retrieves the configured segment retention time in milliseconds.
     * If the value is not set in the properties, a default value of 30000 milliseconds is returned.
     *
     * @return the segment retention time in milliseconds
     */
    public int getSegmentRetainTimeMilliseconds() {
        return object2Int(prop.getOrDefault(SEGMENT_RETAIN_TIME_MILLISECONDS, 30000));
    }

}
