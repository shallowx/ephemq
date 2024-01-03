package org.meteor.client.consumer;

public interface Consumer {
    void start();
    void subscribe(String topic, String queue);
    void cancelSubscribe(String topic, String queue);
    void clear(String topic);
    void close() throws InterruptedException;
}
