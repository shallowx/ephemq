package org.shallow.consumer.push;

import io.netty.buffer.ByteBuf;
import org.shallow.Message;
import org.shallow.internal.Listener;
import org.shallow.invoke.ClientChannel;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.proto.notify.NodeOfflineSignal;
import org.shallow.proto.notify.PartitionChangedSignal;
import org.shallow.proto.server.SendMessageExtras;
import org.shallow.util.ByteBufUtil;

import static org.shallow.util.ProtoBufUtil.readProto;

final class PushConsumerListener implements Listener {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(PushConsumerListener.class);

    private MessagePushListener listener;

    public PushConsumerListener() {

    }

    public void registerListener(MessagePushListener listener) {
        this.listener = listener;
    }

    @Override
    public void onPartitionChanged(ClientChannel channel, PartitionChangedSignal signal) {

    }

    @Override
    public void onNodeOffline(ClientChannel channel, NodeOfflineSignal signal) {

    }

    @Override
    public void onPushMessage(short version, String topic, String queue, int epoch, long index, ByteBuf data) {
        try {
            SendMessageExtras extras = readProto(data, SendMessageExtras.parser());

            byte[] body = ByteBufUtil.buf2Bytes(data);
            Message message = new Message(topic, queue, version, body, epoch, index, new Message.Extras(extras.getExtrasMap()));
            listener.onMessage(message);
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error(t.getMessage(), t);
            }
        }
    }
}
