package org.leopard.client.consumer.push;

import org.leopard.client.Message;
import org.leopard.client.consumer.ConsumeListener;

@FunctionalInterface
public interface MessagePushListener extends ConsumeListener {
    void onMessage(Message message);
}
