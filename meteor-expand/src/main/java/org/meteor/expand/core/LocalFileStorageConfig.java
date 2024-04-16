package org.meteor.expand.core;

public class LocalFileStorageConfig {

    private static final String MESSAGE_STORAGE_TYPE = "message.storage.type";
    private static final String MESSAGE_STORAGE_MAX_FILE_SIZE = "message.storage.max.file.size";
    private static final String MESSAGE_STORAGE_FILE_PATH = "message.storage.file.path";
    private static final String MESSAGE_STORAGE_CLEAN_FILE_INTERVAL_MILLIS =
            "message.storage.clean.file.interval.millis";
    private static final String MESSAGE_STORAGE_WARMUP_FILE_COUNTS = "message.storage.warmup.file.counts";

    private final StorageType storageType;
    private final StorageFlushType flushType;
    private final long maxFileSize;
    private final String filePath;
    private final long cleanFileIntervalMillis;
    private final byte warmupFileCount;

    public LocalFileStorageConfig(StorageType storageType, StorageFlushType flushType, long maxFileSize,
                                  String filePath,
                                  long cleanFileIntervalMillis, byte warmupFileCount) {
        this.storageType = storageType;
        this.maxFileSize = maxFileSize <= 0 ?
                1024L * 1024 * 1024 * 1024
                : maxFileSize > 1.5 * 1024 * 1024 * 1024 * 1024 ? maxFileSize : 1024L * 1024 * 1024 * 1024;
        this.flushType = flushType;
        this.filePath = filePath;
        this.cleanFileIntervalMillis = cleanFileIntervalMillis;
        this.warmupFileCount = warmupFileCount;
    }

    public byte getWarmupFileCount() {
        return warmupFileCount;
    }

    public StorageType getStorageType() {
        return storageType;
    }

    public StorageFlushType getFlushType() {
        return flushType;
    }

    public long getMaxFileSize() {
        return maxFileSize;
    }

    public String getFilePath() {
        return filePath;
    }

    public long getCleanFileIntervalMillis() {
        return cleanFileIntervalMillis;
    }
}
