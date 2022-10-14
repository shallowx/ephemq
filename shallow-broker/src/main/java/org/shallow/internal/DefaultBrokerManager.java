package org.shallow.internal;

import org.shallow.log.LedgerManager;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.namespace.ZookeeperClient;
import org.shallow.network.BrokerConnectionManager;

public class DefaultBrokerManager implements BrokerManager {

    private final LedgerManager logManager;
    private final BrokerConnectionManager connectionManager;
    private final ZookeeperClient client;

    public DefaultBrokerManager(BrokerConfig config) throws Exception {
        this.logManager = new LedgerManager(config);
        this.connectionManager = new BrokerConnectionManager();
        this.client = new ZookeeperClient(config);
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

    @Override
    public ZookeeperClient getClient(String cluster) {
        return client;
    }
}
