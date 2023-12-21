package org.meteor.config;

import org.meteor.common.internal.TypeTransformUtil;

import java.util.Properties;

public class SegmentConfig {
    private static final String SEGMENT_ROLLING_SIZE = "segment.rolling.size";
    private static final String SEGMENT_RETAIN_LIMIT = "segment.retain.limit";
    private static final String SEGMENT_RETAIN_TIME = "segment.retain.time.ms";

    private final Properties prop;

    public SegmentConfig(Properties prop) {
        this.prop = prop;
    }

    public int getSegmentRollingSize() {
        return TypeTransformUtil.object2Int(prop.getOrDefault(SEGMENT_ROLLING_SIZE, 4194304));
    }

    public int getSegmentRetainLimit() {
        return TypeTransformUtil.object2Int(prop.getOrDefault(SEGMENT_RETAIN_LIMIT, 3));
    }

    public int getSegmentRetainTime() {
        return TypeTransformUtil.object2Int(prop.getOrDefault(SEGMENT_RETAIN_TIME, 30000));
    }

}
