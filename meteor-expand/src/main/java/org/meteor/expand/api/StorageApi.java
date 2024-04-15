package org.meteor.expand.api;

import org.meteor.expand.core.Storage;

public interface StorageApi {
    Storage load(String topic, String queue, int ledger);

    boolean write(Storage message);
}
