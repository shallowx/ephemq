package org.ostara.client.consumer;

import org.ostara.common.metadata.Subscription;

@FunctionalInterface
public interface SubscribeCallback {
    void onCompleted(Subscription subscription, Throwable cause);
}
