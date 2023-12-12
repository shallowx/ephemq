package org.meteor.client.consumer;

import io.netty.buffer.ByteBuf;
import org.meteor.common.Extras;
import org.meteor.common.MessageId;

@FunctionalInterface
public interface MessageListener {
    void onMessage(String topic, String queue, MessageId messageId, ByteBuf message, Extras extras);
}
