package org.leopard.client.producer;

@FunctionalInterface
public interface SendCallback {
    void onCompleted(SendResult sendResult, Throwable cause);
}
