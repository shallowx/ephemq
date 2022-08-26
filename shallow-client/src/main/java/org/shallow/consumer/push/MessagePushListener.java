package org.shallow.consumer.push;

import io.netty.buffer.ByteBuf;

@FunctionalInterface
public interface MessagePushListener {
    void onMessage(String topic, String queue, ByteBuf message);
}
