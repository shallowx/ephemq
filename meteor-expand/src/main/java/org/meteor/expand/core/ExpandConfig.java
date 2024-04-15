package org.meteor.expand.core;

public class ExpandConfig {

    private static final String MESSAGE_STORAGE_TYPE = "message.storage.type";
    private static final String MESSAGE_STORAGE_MAX_FILE_SIZE = "message.storage.max.file.size";
    private static final String MESSAGE_STORAGE_FILE_PATH = "message.storage.file.path";

    private StorageType storageType;
    private StorageFlushType flushType;
    private long maxFileSize;
    private String filePath;

    public ExpandConfig(StorageType storageType, StorageFlushType flushType, long maxFileSize, String filePath) {
        this.storageType = storageType;
        this.maxFileSize = maxFileSize <= 0 ?
                1024L * 1024 * 1024 * 1024
                : maxFileSize > 1.5 * 1024 * 1024 * 1024 * 1024 ? maxFileSize : 1024L * 1024 * 1024 * 1024;
        this.flushType = flushType;
        this.filePath = filePath;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
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
