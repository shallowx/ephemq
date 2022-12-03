package org.ostara.client.consumer;

@FunctionalInterface
public interface CleanSubscribeCallback {
    void onCompleted(Throwable t);
}
