package org.shallow.metadata.listener;

public interface ClusterListener {

    void onServerRegistration(String nodeId, String host, int post);
    void onServerOffline(String nodeId, String host, int port);

}
