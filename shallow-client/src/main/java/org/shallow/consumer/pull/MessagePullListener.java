package org.shallow.consumer.pull;

@FunctionalInterface
public interface MessagePullListener {
    void onMessage(PullResult result);
}
