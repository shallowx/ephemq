package org.shallow.client.consumer.pull;

import org.shallow.client.consumer.ConsumeListener;

@FunctionalInterface
public interface MessagePullListener extends ConsumeListener {
    void onMessage(PullResult result);
}
