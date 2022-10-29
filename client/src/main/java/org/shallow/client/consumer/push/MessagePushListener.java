package org.shallow.client.consumer.push;

import org.shallow.client.Message;
import org.shallow.client.consumer.ConsumeListener;

@FunctionalInterface
public interface MessagePushListener extends ConsumeListener {
    void onMessage(Message message);
}
