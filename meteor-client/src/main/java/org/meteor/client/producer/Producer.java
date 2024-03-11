package org.meteor.client.producer;

import io.netty.buffer.ByteBuf;
import org.meteor.common.message.MessageId;

import java.util.Map;

public interface Producer {
    void start();

    MessageId send(String topic, String queue, ByteBuf message, Map<String, String> extras);

    MessageId send(String topic, String queue, ByteBuf message, Map<String, String> extras, long timeout);

    void sendAsync(String topic, String queue, ByteBuf message, Map<String, String> extras, SendCallback callback);

    void sendOneway(String topic, String queue, ByteBuf message, Map<String, String> extras);

    void close();
}
