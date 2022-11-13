package org.leopard.client.internal;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.leopard.client.consumer.ConsumeListener;
import org.leopard.client.consumer.MessagePostInterceptor;
import org.leopard.remote.proto.notify.NodeOfflineSignal;
import org.leopard.remote.proto.notify.PartitionChangedSignal;

public interface ClientListener {

    default void onPartitionChanged(ClientChannel channel, PartitionChangedSignal signal){}

    @SuppressWarnings("unused")
    default void onNodeOffline(ClientChannel channel, NodeOfflineSignal signal){}

    default void onPushMessage(Channel channel, int ledgerId, short version, String topic, String queue, int epoch, long index, ByteBuf data){}

    default void onPullMessage(Channel channel, int ledgerId, String topic, String queue, int ledger, int limit, int epoch, long index, ByteBuf data){}

    default void registerListener(ConsumeListener listener) {}

    default void registerInterceptor(MessagePostInterceptor filter) {}
}
