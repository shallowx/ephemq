package org.shallow.consumer.push;

import org.shallow.Message;
import org.shallow.consumer.ConsumeListener;

@FunctionalInterface
public interface MessagePushListener extends ConsumeListener {
    void onMessage(Message message);
}
