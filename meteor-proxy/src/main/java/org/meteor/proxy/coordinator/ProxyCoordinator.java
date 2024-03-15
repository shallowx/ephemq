package org.meteor.proxy.coordinator;

import org.meteor.coordinator.Coordinator;

public interface ProxyCoordinator extends Coordinator {
    LedgerSyncCoordinator getLedgerSyncCoordinator();
}
