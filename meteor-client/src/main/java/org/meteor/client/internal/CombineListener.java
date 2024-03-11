package org.meteor.client.internal;

import io.netty.buffer.ByteBuf;
import org.meteor.remote.proto.client.MessagePushSignal;
import org.meteor.remote.proto.client.NodeOfflineSignal;
import org.meteor.remote.proto.client.SyncMessageSignal;
import org.meteor.remote.proto.client.TopicChangedSignal;

public interface CombineListener {
    default void onChannelActive(ClientChannel channel) {
    }

    default void onChannelClosed(ClientChannel channel) {
    }

    default void onPushMessage(ClientChannel channel, MessagePushSignal signal, ByteBuf data) {
    }

    default void onTopicChanged(ClientChannel channel, TopicChangedSignal signal) {
    }

    default void onNodeOffline(ClientChannel channel, NodeOfflineSignal signal) {
    }

    default void onSyncMessage(ClientChannel channel, SyncMessageSignal signal, ByteBuf data) {
    }

    default void listenerCompleted() throws InterruptedException {
    }
}
