package org.shallow.consumer.push;

@FunctionalInterface
public interface SubscribeCallback {
    void onCompleted(Subscription subscription, Throwable cause);
}
