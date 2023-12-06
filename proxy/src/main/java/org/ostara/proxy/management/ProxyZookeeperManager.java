package org.ostara.proxy.management;

import org.ostara.management.Manager;

public interface ProxyZookeeperManager extends Manager {
    LedgerSyncManager getLedgerSyncManager();
}
