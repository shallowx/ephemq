package org.shallow.producer;

@FunctionalInterface
public interface MessageFilter {
    void filter(String topic, String queue, Message message);
}
