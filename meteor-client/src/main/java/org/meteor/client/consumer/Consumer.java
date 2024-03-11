package org.meteor.client.consumer;

import java.util.Map;

public interface Consumer {
    void start();

    void subscribe(String topic, String queue);

    void subscribe(Map<String, String> ships);

    void cancelSubscribe(String topic, String queue);

    void clear(String topic);

    void close() throws InterruptedException;
}
