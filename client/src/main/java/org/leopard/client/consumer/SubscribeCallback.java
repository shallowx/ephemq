package org.leopard.client.consumer;

import org.leopard.common.metadata.Subscription;

@FunctionalInterface
public interface SubscribeCallback {
    void onCompleted(Subscription subscription, Throwable cause);
}
