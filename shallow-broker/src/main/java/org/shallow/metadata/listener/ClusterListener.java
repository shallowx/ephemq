package org.shallow.metadata.listener;

public interface ClusterListener {
    void nodeOffline(String nodeId, String host, int port);
}
