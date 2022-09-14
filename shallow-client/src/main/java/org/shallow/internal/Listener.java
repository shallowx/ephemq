package org.shallow.internal;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.shallow.consumer.ConsumeListener;
import org.shallow.consumer.MessagePostInterceptor;
import org.shallow.proto.notify.NodeOfflineSignal;
import org.shallow.proto.notify.PartitionChangedSignal;

public interface Listener {

    default void onPartitionChanged(ClientChannel channel, PartitionChangedSignal signal){}

    @SuppressWarnings("unused")
    default void onNodeOffline(ClientChannel channel, NodeOfflineSignal signal){}

    default void onPushMessage(Channel channel, int ledgerId, short version, String topic, String queue, int epoch, long index, ByteBuf data){}

    default void onPullMessage(Channel channel, int ledgerId, String topic, String queue, int ledger, int limit, int epoch, long index, ByteBuf data){}

    default void registerListener(ConsumeListener listener) {}

    default void registerInterceptor(MessagePostInterceptor filter) {}
}
