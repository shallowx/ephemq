package org.meteor.proxy.coordinatior;

import org.meteor.coordinatior.Coordinator;

public interface ProxyCoordinator extends Coordinator {
    LedgerSyncCoordinator getLedgerSyncCoordinator();
}
