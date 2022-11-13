package org.leopard.client.consumer.pull;

import org.leopard.client.consumer.ConsumeListener;

@FunctionalInterface
public interface MessagePullListener extends ConsumeListener {
    void onMessage(PullResult result);
}
