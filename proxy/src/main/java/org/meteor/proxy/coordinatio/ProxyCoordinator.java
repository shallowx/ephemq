package org.meteor.proxy.coordinatio;

import org.meteor.coordinatio.Coordinator;

public interface ProxyCoordinator extends Coordinator {
    LedgerSyncCoordinator getLedgerSyncManager();
}
