package org.shallow.internal;

import org.shallow.log.LedgerManager;
import org.shallow.network.BrokerConnectionManager;

public interface BrokerManager {
    void start() throws Exception;
    void shutdownGracefully() throws Exception;

    LedgerManager getLedgerManager();

    BrokerConnectionManager getBrokerConnectionManager();
}
