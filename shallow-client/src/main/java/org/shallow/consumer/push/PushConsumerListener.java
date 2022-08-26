package org.shallow.consumer.push;

import io.netty.buffer.ByteBuf;
import org.shallow.internal.Listener;
import org.shallow.invoke.ClientChannel;
import org.shallow.proto.notify.NodeOfflineSignal;
import org.shallow.proto.notify.PartitionChangedSignal;

public class PushConsumerListener implements Listener {

    private MessagePushListener listener;

    public PushConsumerListener(PushConsumer consumer) {

    }

    @Override
    public void onPartitionChanged(ClientChannel channel, PartitionChangedSignal signal) {

    }

    @Override
    public void onNodeOffline(ClientChannel channel, NodeOfflineSignal signal) {

    }

    @Override
    public void onPushMessage(ByteBuf data) {
        listener.onMessage(null, null, data);
    }
}
