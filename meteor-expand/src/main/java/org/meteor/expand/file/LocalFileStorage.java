package org.meteor.expand.file;

import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.expand.api.AbstractStorage;
import org.meteor.expand.core.LocalFileStorageConfig;
import org.meteor.expand.core.Storage;

public class LocalFileStorage extends AbstractStorage {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(LocalFileStorage.class);

    private static final String TAIL = "-91912932";
    private final int WARM_UP = 1024 * 4;
    private final ExecutorService cleanExecutor =
            Executors.newSingleThreadExecutor(new DefaultThreadFactory("local-storage-clean"));
    private final LocalFileStorageConfig config;

    public LocalFileStorage(LocalFileStorageConfig config) {
        this.config = config;
    }

    public void start() {
    }

    private void createAndWarUp(byte total) {
        
    }

    @Override
    protected boolean doWrite(Storage message) {
        return true;
    }

    @Override
    protected Storage doLoad(String topic, String queue, int ledger) {
        return Storage.EMPTY_STORAGE;
    }


    public void shutdownGracefully() {
        if (!cleanExecutor.isShutdown() || !cleanExecutor.isTerminated()) {
            cleanExecutor.shutdown();
        }
    }
}
