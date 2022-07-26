package org.shallow.internal;

@SuppressWarnings("all")
public interface BrokerManager {

    void start() throws Exception;
    void shutdownGracefully() throws Exception;

}
