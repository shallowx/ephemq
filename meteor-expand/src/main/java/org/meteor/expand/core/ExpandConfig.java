package org.meteor.expand.core;

public class ExpandConfig {

    private static final String MESSAGE_STORAGE_TYPE = "message.storage.type";
    private static final String MESSAGE_STORAGE_MAX_FILE_SIZE = "message.storage.max.file.size";

    private StorageType storageType;
    private StorageFlushType flushType;
    public long maxFileSize;

    public ExpandConfig(StorageType storageType, StorageFlushType flushType, long maxFileSize) {
        this.storageType = storageType;
        this.maxFileSize = maxFileSize <= 0 ?
                1024L * 1024 * 1024 * 1024
                : maxFileSize > 1.5 * 1024 * 1024 * 1024 * 1024 ? maxFileSize : 1024L * 1024 * 1024 * 1024;
        this.flushType = flushType;
    }

    public StorageType getStorageType() {
        return storageType;
    }

    public void setStorageType(StorageType storageType) {
        this.storageType = storageType;
    }

    public StorageFlushType getFlushType() {
        return flushType;
    }

    public void setFlushType(StorageFlushType flushType) {
        this.flushType = flushType;
    }

    public long getMaxFileSize() {
        return maxFileSize;
    }

    public void setMaxFileSize(long maxFileSize) {
        this.maxFileSize = maxFileSize;
    }
}
