package org.shallow.consumer.push;

@FunctionalInterface
public interface CleanSubscribeCallback {
    void onCompleted(Throwable t);
}
