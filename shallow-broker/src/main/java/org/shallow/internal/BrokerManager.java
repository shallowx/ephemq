package org.shallow.internal;

import org.shallow.internal.client.QuorumVoterClient;
import org.shallow.metadata.MappedFileApi;
import org.shallow.metadata.management.ClusterManager;
import org.shallow.metadata.management.TopicManager;
import org.shallow.metadata.sraft.SRaftProcessController;
import org.shallow.pool.ShallowChannelPool;

@SuppressWarnings("all")
public interface BrokerManager {
    void start() throws Exception;
    void shutdownGracefully() throws Exception;

    QuorumVoterClient getQuorumVoterClient();

    MappedFileApi getMappedFileApi();

    SRaftProcessController getController();
}
