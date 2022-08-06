package org.shallow.nraft;

public interface HeartbeatListener {
    void receive(long time);

    long getHeartbeatTime();

    void registerHeartbeat();
}
