package org.meteor.proxy.coordinatior;

import org.meteor.coordinator.Coordinator;

public interface ProxyCoordinator extends Coordinator {
    LedgerSyncCoordinator getLedgerSyncCoordinator();
}
