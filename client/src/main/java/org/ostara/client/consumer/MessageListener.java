package org.ostara.client.consumer;

import org.ostara.client.Message;

@FunctionalInterface
public interface MessageListener extends ConsumeListener {
    void onMessage(Message message);
}
