package org.meteor.config;

import org.meteor.common.util.TypeTransformUtils;

import java.util.Properties;

public class ChunkRecordDispatchConfig {
    private static final String CHUNK_DISPATCH_ENTRY_LOAD_LIMIT = "chunk.dispatch.entry.load.limit";
    private static final String CHUNK_DISPATCH_ENTRY_FOLLOW_LIMIT = "chunk.dispatch.entry.follow.limit";
    private static final String CHUNK_DISPATCH_ENTRY_PURSUE_LIMIT = "chunk.dispatch.entry.pursue.limit";
    private static final String CHUNK_DISPATCH_ENTRY_ALIGN_LIMIT = "chunk.dispatch.entry.align.limit";
    private static final String CHUNK_DISPATCH_ENTRY_PURSUE_TIMEOUT_MS = "chunk.dispatch.entry.pursue.timeout.ms";
    private static final String CHUNK_DISPATCH_ENTRY_BYTES_LIMIT = "chunk.dispatch.entry.bytes.limit";
    private final Properties prop;

    public ChunkRecordDispatchConfig(Properties prop) {
        this.prop = prop;
    }

    public int getChunkDispatchEntryBytesLimit() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(CHUNK_DISPATCH_ENTRY_BYTES_LIMIT, 65536));
    }

    public int getChunkDispatchEntryLoadLimit() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(CHUNK_DISPATCH_ENTRY_LOAD_LIMIT, 50));
    }

    public int getChunkDispatchEntryFollowLimit() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(CHUNK_DISPATCH_ENTRY_FOLLOW_LIMIT, 100));
    }

    public int getChunkDispatchEntryPursueLimit() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(CHUNK_DISPATCH_ENTRY_PURSUE_LIMIT, 500));
    }

    public int getChunkDispatchEntryAlignLimit() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(CHUNK_DISPATCH_ENTRY_ALIGN_LIMIT, 2000));
    }

    public int getChunkDispatchEntryPursueTimeoutMs() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(CHUNK_DISPATCH_ENTRY_PURSUE_TIMEOUT_MS, 10000));
    }
}
