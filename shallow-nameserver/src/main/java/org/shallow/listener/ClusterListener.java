package org.shallow.listener;

public interface ClusterListener {
    void onJoin(String name, String host, int port);
    void onOffline();
}

