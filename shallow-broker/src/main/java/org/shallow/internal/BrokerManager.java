package org.shallow.internal;

import org.shallow.log.LedgerManager;
import org.shallow.metadata.sraft.SRaftQuorumVoterClient;
import org.shallow.metadata.MappedFileApi;
import org.shallow.metadata.sraft.SRaftProcessController;
import org.shallow.network.BrokerConnectionManager;

public interface BrokerManager {
    void start() throws Exception;
    void shutdownGracefully() throws Exception;

    SRaftQuorumVoterClient getQuorumVoterClient();

    MappedFileApi getMappedFileApi();

    SRaftProcessController getController();

    LedgerManager getLogManager();

    BrokerConnectionManager getBrokerConnectionManager();
}
