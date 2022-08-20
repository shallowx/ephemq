package org.shallow.internal;

import org.shallow.log.LogManager;
import org.shallow.metadata.sraft.SRaftQuorumVoterClient;
import org.shallow.metadata.MappedFileApi;
import org.shallow.metadata.sraft.SRaftProcessController;

public interface BrokerManager {
    void start() throws Exception;
    void shutdownGracefully() throws Exception;

    SRaftQuorumVoterClient getQuorumVoterClient();

    MappedFileApi getMappedFileApi();

    SRaftProcessController getController();

    LogManager getLogManager();
}
