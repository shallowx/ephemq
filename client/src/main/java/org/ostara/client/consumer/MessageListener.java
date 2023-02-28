package org.ostara.client.consumer;

import org.ostara.client.Message;

@FunctionalInterface
public interface MessageListener extends ConsumerListener {
    void onMessage(Message message);
}
