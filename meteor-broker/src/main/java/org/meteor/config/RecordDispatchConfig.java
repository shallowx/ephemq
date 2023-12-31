package org.meteor.config;

import java.util.Properties;
import static org.meteor.common.util.ObjectLiteralsTransformUtil.object2Int;

public class RecordDispatchConfig {
    private static final String DISPATCH_ENTRY_LOAD_LIMIT = "dispatch.entry.load.limit";
    private static final String DISPATCH_ENTRY_FOLLOW_LIMIT = "dispatch.entry.follow.limit";
    private static final String DISPATCH_ENTRY_PURSUE_LIMIT = "dispatch.entry.pursue.limit";
    private static final String DISPATCH_ENTRY_ALIGN_LIMIT = "dispatch.entry.align.limit";
    private static final String DISPATCH_ENTRY_PURSUE_TIMEOUT_MILLISECONDS = "dispatch.entry.pursue.timeout.milliseconds";
    private final Properties prop;

    public RecordDispatchConfig(Properties prop) {
        this.prop = prop;
    }

    public int getDispatchEntryLoadLimit() {
        return object2Int(prop.getOrDefault(DISPATCH_ENTRY_LOAD_LIMIT, 50));
    }

    public int getDispatchEntryFollowLimit() {
        return object2Int(prop.getOrDefault(DISPATCH_ENTRY_FOLLOW_LIMIT, 100));
    }

    public int getDispatchEntryPursueLimit() {
        return object2Int(prop.getOrDefault(DISPATCH_ENTRY_PURSUE_LIMIT, 500));
    }

    public int getDispatchEntryAlignLimit() {
        return object2Int(prop.getOrDefault(DISPATCH_ENTRY_ALIGN_LIMIT, 2000));
    }

    public int getDispatchEntryPursueTimeoutMilliseconds() {
        return object2Int(prop.getOrDefault(DISPATCH_ENTRY_PURSUE_TIMEOUT_MILLISECONDS, 10000));
    }
}
