package org.ostara.client.consumer;

import io.netty.buffer.ByteBuf;
import org.ostara.common.Extras;
import org.ostara.common.MessageId;

@FunctionalInterface
public interface MessageListener {
    void onMessage(String topic, String queue, MessageId messageId, ByteBuf message, Extras extras);
}
