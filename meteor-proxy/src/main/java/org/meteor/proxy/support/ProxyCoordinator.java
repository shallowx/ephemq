package org.meteor.proxy.support;

import org.meteor.support.Coordinator;

public interface ProxyCoordinator extends Coordinator {
    LedgerSyncCoordinator getLedgerSyncCoordinator();
}
