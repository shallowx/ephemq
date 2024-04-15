package org.meteor.expand.api;

import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.expand.core.Storage;

public abstract class AbstractStorage implements StorageApi {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(AbstractStorage.class);

    @Override
    public Storage load(String topic, String queue, int ledger) {
        if (topic == null || topic.isEmpty()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Storage topic is null or empty");
            }
            return Storage.EMPTY_STORAGE;
        }

        if (queue == null || queue.isEmpty()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Storage queue is null or empty");
            }
            return Storage.EMPTY_STORAGE;
        }

        if (ledger < 0) {
            if (logger.isDebugEnabled()) {
                logger.debug("Storage ledger is negative");
            }
            return Storage.EMPTY_STORAGE;
        }
        return doLoad(topic, queue, ledger);
    }

    @Override
    public boolean write(Storage message) {
        boolean allowWrite = checkStorage(message);
        if (allowWrite) {
            return doWrite(message);
        }
        return false;
    }

    private boolean checkStorage(Storage storage) {
        if (storage.topic() == null || storage.topic().isEmpty()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Storage topic is null or empty");
            }
            return false;
        }

        if (storage.queue() == null || storage.queue().isEmpty()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Storage queue is null or empty");
            }
            return false;
        }

        if (storage.ledger() < 0) {
            if (logger.isDebugEnabled()) {
                logger.debug("Storage ledger is negative");
            }
        }
        return storage.message() != null && storage.message().length != 0;
    }

    protected abstract boolean doWrite(Storage message);

    protected abstract Storage doLoad(String topic, String queue, int ledger);
}
