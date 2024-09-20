package org.meteor.proxy.support;

import org.meteor.support.Manager;

/**
 * The ProxyManager interface extends the Manager interface to provide additional functionality
 * specifically related to proxy management. It introduces a method for retrieving the ledger
 * synchronization support.
 */
public interface ProxyManager extends Manager {
    /**
     * Retrieves the synchronization support for ledgers.
     *
     * @return the LedgerSyncSupport instance that provides methods and functionality to synchronize ledgers.
     */
    LedgerSyncSupport getLedgerSyncSupport();
}
