package org.shallow.producer;

@FunctionalInterface
public interface SendCallback {
    void onCompleted(SendResult sendResult, Throwable cause);
}
