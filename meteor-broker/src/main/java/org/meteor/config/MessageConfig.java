package org.meteor.config;

import io.netty.util.NettyRuntime;
import java.util.Properties;

import static org.meteor.common.util.ObjectLiteralsTransformUtil.object2Int;

public class MessageConfig {
    private static final String MESSAGE_SYNC_THREAD_LIMIT = "message.sync.thread.limit";
    private static final String MESSAGE_STORAGE_THREAD_LIMIT = "message.storage.thread.limit";
    private static final String MESSAGE_DISPATCH_THREAD_LIMIT = "message.dispatch.thread.limit";
    private final Properties prop;

    public MessageConfig(Properties prop) {
        this.prop = prop;
    }

    public int getMessageSyncThreadLimit() {
        return object2Int(prop.getOrDefault(MESSAGE_SYNC_THREAD_LIMIT, NettyRuntime.availableProcessors()));
    }

    public int getMessageStorageThreadLimit() {
        return object2Int(prop.getOrDefault(MESSAGE_STORAGE_THREAD_LIMIT, NettyRuntime.availableProcessors()));
    }

    public int getMessageDispatchThreadLimit() {
        return object2Int(prop.getOrDefault(MESSAGE_DISPATCH_THREAD_LIMIT, NettyRuntime.availableProcessors()));
    }
}
