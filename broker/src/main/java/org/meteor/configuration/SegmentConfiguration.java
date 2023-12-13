package org.meteor.configuration;

import org.meteor.common.util.TypeTransformUtils;

import java.util.Properties;

public class SegmentConfiguration {
    private static final String SEGMENT_ROLLING_SIZE = "segment.rolling.size";
    private static final String SEGMENT_RETAIN_LIMIT = "segment.retain.limit";
    private static final String SEGMENT_RETAIN_TIME = "segment.retain.time.ms";

    private final Properties prop;

    public SegmentConfiguration(Properties prop) {
        this.prop = prop;
    }

    public int getSegmentRollingSize() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(SEGMENT_ROLLING_SIZE, 4194304));
    }

    public int getSegmentRetainLimit() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(SEGMENT_RETAIN_LIMIT, 3));
    }

    public int getSegmentRetainTime() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(SEGMENT_RETAIN_TIME, 30000));
    }

}
