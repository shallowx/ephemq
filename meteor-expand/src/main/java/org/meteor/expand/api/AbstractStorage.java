package org.meteor.expand.api;

import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.expand.core.Storage;

public abstract class AbstractStorage implements StorageApi {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(AbstractStorage.class);

    @Override
    public Storage load(String topic, String queue) {
        if (topic == null || topic.isEmpty()) {
            return Storage.EMPTY_STORAGE;
        }

        if (queue == null || queue.isEmpty()) {
            return Storage.EMPTY_STORAGE;
        }
        return doLoad(topic, queue);
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
            return false;
        }

        if (storage.queue() == null || storage.queue().isEmpty()) {
            return false;
        }

        return storage.message() != null && storage.message().length != 0;
    }

    protected abstract boolean doWrite(Storage message);

    protected abstract Storage doLoad(String topic, String queue);
}
