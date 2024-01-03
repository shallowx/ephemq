package org.meteor.client.consumer;

import io.netty.buffer.ByteBuf;
import org.meteor.common.message.MessageId;

import java.util.Map;

@FunctionalInterface
public interface MessageListener {
    void onMessage(String topic, String queue, MessageId messageId, ByteBuf message, Map<String, String> extras);
}
