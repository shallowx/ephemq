package org.shallow.producer;

import org.shallow.consumer.MessageListener;

@FunctionalInterface
public interface MessageFilter {
    MessageListener ACTIVE = (topic, queue, message) -> {
        // default: do nothing
    };

    void filter(String topic, String queue);
}
