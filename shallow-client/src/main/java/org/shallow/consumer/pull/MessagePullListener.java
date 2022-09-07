package org.shallow.consumer.pull;

import org.shallow.consumer.ConsumeListener;

@FunctionalInterface
public interface MessagePullListener extends ConsumeListener {
    void onMessage(PullResult result);
}
