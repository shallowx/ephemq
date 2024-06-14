package org.meteor.proxy.support;

import org.meteor.support.Manager;

public interface ProxyManager extends Manager {
    LedgerSyncSupport getLedgerSyncSupport();
}
