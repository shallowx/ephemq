package org.meteor.expand.api;

import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.expand.bookkeeper.BookkeeperStorage;
import org.meteor.expand.core.ExpandConfig;
import org.meteor.expand.core.MeteorExpandStorageException;
import org.meteor.expand.core.StorageType;
import org.meteor.expand.file.LocalFileStorage;

public class StorageApiFactory {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(StorageApiFactory.class);

    private final ExpandConfig config;
    private StorageApi localStorageApi;
    private StorageApi bookkeeperStorageApi;

    public static StorageApiFactory getInstance(ExpandConfig config) {
        return new StorageApiFactory(config);
    }

    private StorageApiFactory(ExpandConfig config) {
        this.config = config;
    }

    public StorageApi getStorageApi(StorageType type) {
        if (type == null) {
            throw new MeteorExpandStorageException("Storage type cannot be empty");
        }

        switch (type) {
            case LOCALE -> {
                if (localStorageApi == null) {
                    localStorageApi = new LocalFileStorage(config.localFileStorageConfig());
                }
                return localStorageApi;
            }
            case BOOKKEEPER -> {
                if (bookkeeperStorageApi == null) {
                    bookkeeperStorageApi = new BookkeeperStorage();
                }
                return bookkeeperStorageApi;
            }
            default -> throw new MeteorExpandStorageException(
                    "Unsupported storage type[" + type + "], and expected storage types are [LOCALE, BOOKKEEPER]");
        }
    }
}
