package org.meteor.proxy.management;

import org.meteor.management.Manager;

public interface ProxyZookeeperManager extends Manager {
    LedgerSyncManager getLedgerSyncManager();
}
