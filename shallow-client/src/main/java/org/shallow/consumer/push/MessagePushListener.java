package org.shallow.consumer.push;

import org.shallow.Message;

@FunctionalInterface
public interface MessagePushListener {
    void onMessage(Message message);
}
