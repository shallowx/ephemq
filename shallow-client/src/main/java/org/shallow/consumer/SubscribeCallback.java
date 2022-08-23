package org.shallow.consumer;

@FunctionalInterface
public interface SubscribeCallback {
    void onCompleted(Subscription subscription, Throwable cause);
}
