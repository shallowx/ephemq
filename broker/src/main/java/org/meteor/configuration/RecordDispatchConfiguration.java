package org.meteor.configuration;

import org.meteor.common.util.TypeTransformUtils;

import java.util.Properties;

public class RecordDispatchConfiguration {
    private static final String DISPATCH_ENTRY_LOAD_LIMIT = "dispatch.entry.load.limit";
    private static final String DISPATCH_ENTRY_FOLLOW_LIMIT = "dispatch.entry.follow.limit";
    private static final String DISPATCH_ENTRY_PURSUE_LIMIT = "dispatch.entry.pursue.limit";
    private static final String DISPATCH_ENTRY_ALIGN_LIMIT = "dispatch.entry.align.limit";
    private static final String DISPATCH_ENTRY_PURSUE_TIMEOUT_MS = "dispatch.entry.pursue.timeout.ms";
    private final Properties prop;

    public RecordDispatchConfiguration(Properties prop) {
        this.prop = prop;
    }

    public int getDispatchEntryLoadLimit() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(DISPATCH_ENTRY_LOAD_LIMIT, 50));
    }

    public int getDispatchEntryFollowLimit() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(DISPATCH_ENTRY_FOLLOW_LIMIT, 100));
    }

    public int getDispatchEntryPursueLimit() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(DISPATCH_ENTRY_PURSUE_LIMIT, 500));
    }

    public int getDispatchEntryAlignLimit() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(DISPATCH_ENTRY_ALIGN_LIMIT, 2000));
    }

    public int getDispatchEntryPursueTimeoutMs() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(DISPATCH_ENTRY_PURSUE_TIMEOUT_MS, 10000));
    }
}
