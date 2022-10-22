package org.shallow.client.producer;

@FunctionalInterface
public interface SendCallback {
    void onCompleted(SendResult sendResult, Throwable cause);
}
