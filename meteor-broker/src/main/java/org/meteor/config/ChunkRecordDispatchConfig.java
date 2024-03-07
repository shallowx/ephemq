package org.meteor.config;

import java.util.Properties;

import static org.meteor.common.util.ObjectLiteralsTransformUtil.object2Int;

public class ChunkRecordDispatchConfig {
    private static final String CHUNK_DISPATCH_ENTRY_LOAD_LIMIT = "chunk.dispatch.entry.load.limit";
    private static final String CHUNK_DISPATCH_ENTRY_FOLLOW_LIMIT = "chunk.dispatch.entry.follow.limit";
    private static final String CHUNK_DISPATCH_ENTRY_PURSUE_LIMIT = "chunk.dispatch.entry.pursue.limit";
    private static final String CHUNK_DISPATCH_ENTRY_ALIGN_LIMIT = "chunk.dispatch.entry.align.limit";
    private static final String CHUNK_DISPATCH_ENTRY_PURSUE_TIMEOUT_MILLISECONDS = "chunk.dispatch.entry.pursue.timeout.milliseconds";
    private static final String CHUNK_DISPATCH_ENTRY_BYTES_LIMIT = "chunk.dispatch.entry.bytes.limit";
    private static final String CHUNK_SYNC_SEMAPHORE_LIMIT = "chunk.sync.semaphore.limit";
    private final Properties prop;

    public ChunkRecordDispatchConfig(Properties prop) {
        this.prop = prop;
    }

    public int getChunkDispatchSyncSemaphore() {
        return object2Int(prop.getOrDefault(CHUNK_SYNC_SEMAPHORE_LIMIT, 100));
    }

    public int getChunkDispatchEntryBytesLimit() {
        return object2Int(prop.getOrDefault(CHUNK_DISPATCH_ENTRY_BYTES_LIMIT, 65536));
    }

    public int getChunkDispatchEntryLoadLimit() {
        return object2Int(prop.getOrDefault(CHUNK_DISPATCH_ENTRY_LOAD_LIMIT, 50));
    }

    public int getChunkDispatchEntryFollowLimit() {
        return object2Int(prop.getOrDefault(CHUNK_DISPATCH_ENTRY_FOLLOW_LIMIT, 100));
    }

    public int getChunkDispatchEntryPursueLimit() {
        return object2Int(prop.getOrDefault(CHUNK_DISPATCH_ENTRY_PURSUE_LIMIT, 500));
    }

    public int getChunkDispatchEntryAlignLimit() {
        return object2Int(prop.getOrDefault(CHUNK_DISPATCH_ENTRY_ALIGN_LIMIT, 2000));
    }

    public int getChunkDispatchEntryPursueTimeoutMilliseconds() {
        return object2Int(prop.getOrDefault(CHUNK_DISPATCH_ENTRY_PURSUE_TIMEOUT_MILLISECONDS, 10000));
    }
}
