package org.meteor.config;

import java.util.Properties;
import static org.meteor.common.util.ObjectLiteralsTransformUtil.object2Int;

public class SegmentConfig {
    private static final String SEGMENT_ROLLING_SIZE = "segment.rolling.size";
    private static final String SEGMENT_RETAIN_LIMIT = "segment.retain.limit";
    private static final String SEGMENT_RETAIN_TIME_MILLISECONDS = "segment.retain.time.milliseconds";

    private final Properties prop;

    public SegmentConfig(Properties prop) {
        this.prop = prop;
    }

    public int getSegmentRollingSize() {
        return object2Int(prop.getOrDefault(SEGMENT_ROLLING_SIZE, 4194304));
    }

    public int getSegmentRetainLimit() {
        return object2Int(prop.getOrDefault(SEGMENT_RETAIN_LIMIT, 3));
    }

    public int getSegmentRetainTimeMilliseconds() {
        return object2Int(prop.getOrDefault(SEGMENT_RETAIN_TIME_MILLISECONDS, 30000));
    }

}
