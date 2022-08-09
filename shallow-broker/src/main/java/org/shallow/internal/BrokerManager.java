package org.shallow.internal;

import org.shallow.metadata.sraft.SRaftProcessController;

@SuppressWarnings("all")
public interface BrokerManager {
    void start() throws Exception;
    void shutdownGracefully() throws Exception;

    SRaftProcessController getController();
}
