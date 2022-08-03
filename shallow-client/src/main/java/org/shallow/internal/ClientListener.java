package org.shallow.internal;

import org.shallow.invoke.ClientChannel;
import org.shallow.proto.notify.NodeOfflineSignal;
import org.shallow.proto.notify.PartitionChangedSignal;

public interface ClientListener {
    default void onPartitionChanged(ClientChannel channel, PartitionChangedSignal signal){}
    default void onNodeOffline(ClientChannel channel, NodeOfflineSignal signal){}
}
