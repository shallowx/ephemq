package org.leopard.client.consumer;

import org.leopard.client.Message;

@FunctionalInterface
public interface MessageListener extends ConsumeListener {
    void onMessage(Message message);
}
