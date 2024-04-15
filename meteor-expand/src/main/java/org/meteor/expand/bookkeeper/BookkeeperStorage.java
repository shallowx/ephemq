package org.meteor.expand.bookkeeper;

import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.expand.api.AbstractStorage;
import org.meteor.expand.core.Storage;

public class BookkeeperStorage extends AbstractStorage {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(BookkeeperStorage.class);

    @Override
    protected boolean doWrite(Storage message) {
        return false;
    }

    @Override
    protected Storage doLoad(String topic, String queue, int ledger) {
        return Storage.EMPTY_STORAGE;
    }
}
