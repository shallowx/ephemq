package org.meteor.expand.file;

import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.expand.api.AbstractStorage;
import org.meteor.expand.core.Storage;

public class LocalFileStorage extends AbstractStorage {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(LocalFileStorage.class);

    private static final String TAIL = "";
    private final long maxFileSize;
    private long currentFileSize;

    public LocalFileStorage(long maxFileSize) {
        this.maxFileSize = maxFileSize;
    }

    public void start() {
        this.currentFileSize = 0;
    }

    @Override
    protected boolean doWrite(Storage message) {
        return true;
    }

    @Override
    protected Storage doLoad(String topic, String queue) {
        return Storage.EMPTY_STORAGE;
    }

    private void write2Tail(long bytes) {
        if (currentFileSize + bytes > maxFileSize) {
            // TODO
        }
        currentFileSize += bytes;
    }

    public void shutdownGracefully() {

    }
}
