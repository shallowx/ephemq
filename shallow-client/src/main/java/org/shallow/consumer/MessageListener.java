package org.shallow.consumer;

import io.netty.buffer.ByteBuf;

@FunctionalInterface
public interface MessageListener {
    void onMessage(String topic, String queue, ByteBuf message);
}
