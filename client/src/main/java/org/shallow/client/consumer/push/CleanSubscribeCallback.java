package org.shallow.client.consumer.push;

@FunctionalInterface
public interface CleanSubscribeCallback {
    void onCompleted(Throwable t);
}
