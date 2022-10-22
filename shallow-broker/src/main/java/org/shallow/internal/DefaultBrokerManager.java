package org.shallow.internal;

import org.shallow.client.ClientConfig;
import org.shallow.log.LedgerManager;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.network.BrokerConnectionManager;

public class DefaultBrokerManager implements BrokerManager {

    private final LedgerManager logManager;
    private final BrokerConnectionManager connectionManager;

    public DefaultBrokerManager(BrokerConfig config) throws Exception {
        ClientConfig quorumVoterClientConfig = new ClientConfig();

        quorumVoterClientConfig.setChannelFixedPoolCapacity(config.getInternalChannelPoolLimit());
        quorumVoterClientConfig.setInvokeExpiredMs(config.getInvokeTimeMs());

        this.logManager = new LedgerManager(config);
        this.connectionManager = new BrokerConnectionManager();
    }

    @Override
    public void start() throws Exception {
        logManager.start();
    }

    @Override
    public LedgerManager getLedgerManager() {
        return logManager;
    }

    @Override
    public BrokerConnectionManager getBrokerConnectionManager() {
        return connectionManager;
    }

    @Override
    public void shutdownGracefully() throws Exception {
        logManager.close();
    }
}
