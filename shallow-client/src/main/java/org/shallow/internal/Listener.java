package org.shallow.internal;

import io.netty.buffer.ByteBuf;
import org.shallow.invoke.ClientChannel;
import org.shallow.proto.notify.NodeOfflineSignal;
import org.shallow.proto.notify.PartitionChangedSignal;

public interface Listener {

    default void onPartitionChanged(ClientChannel channel, PartitionChangedSignal signal){}

    default void onNodeOffline(ClientChannel channel, NodeOfflineSignal signal){}

    default void onPushMessage(ByteBuf data){}

    default void onPullMessage(String topic, String queue, int ledger, int limit, int epoch, long index, ByteBuf data){}
}
