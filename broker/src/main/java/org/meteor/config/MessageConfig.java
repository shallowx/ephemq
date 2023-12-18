package org.meteor.config;

import org.meteor.common.util.TypeTransformUtils;

import java.util.Properties;

public class MessageConfig {
    private static final String MESSAGE_SYNC_THREAD_LIMIT = "message.sync.thread.limit";
    private static final String MESSAGE_STORAGE_THREAD_LIMIT = "message.storage.thread.limit";
    private static final String MESSAGE_DISPATCH_THREAD_LIMIT = "message.dispatch.thread.limit";
    private final Properties prop;

    public MessageConfig(Properties prop) {
        this.prop = prop;
    }

    public int getMessageSyncThreadLimit() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(MESSAGE_SYNC_THREAD_LIMIT, Runtime.getRuntime().availableProcessors()));
    }

    public int getMessageStorageThreadLimit() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(MESSAGE_STORAGE_THREAD_LIMIT, Runtime.getRuntime().availableProcessors()));
    }

    public int getMessageDispatchThreadLimit() {
        return TypeTransformUtils.object2Int(prop.getOrDefault(MESSAGE_DISPATCH_THREAD_LIMIT, Runtime.getRuntime().availableProcessors()));
    }
}
