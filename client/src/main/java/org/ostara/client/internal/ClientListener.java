package org.ostara.client.internal;

import io.netty.buffer.ByteBuf;
import org.ostara.remote.proto.client.MessagePushSignal;
import org.ostara.remote.proto.client.NodeOfflineSignal;
import org.ostara.remote.proto.client.TopicChangedSignal;
public interface ClientListener {
    default void onChannelActive(ClientChannel channel){}
    default void onChannelClosed(ClientChannel channel){}
    default void onPushMessage(ClientChannel channel, MessagePushSignal signal, ByteBuf data){}
    default void onTopicChanged(ClientChannel channel, TopicChangedSignal signal){}
    default void onNodeOffline(ClientChannel channel, NodeOfflineSignal signal){}
}
