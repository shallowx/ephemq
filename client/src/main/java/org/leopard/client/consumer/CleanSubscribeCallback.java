package org.leopard.client.consumer;

@FunctionalInterface
public interface CleanSubscribeCallback {
    void onCompleted(Throwable t);
}
