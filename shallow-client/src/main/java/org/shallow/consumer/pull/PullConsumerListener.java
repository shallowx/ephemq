package org.shallow.consumer.pull;

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

import java.util.ArrayList;
import java.util.List;

import static org.shallow.util.ProtoBufUtil.readProto;

final class PullConsumerListener implements Listener {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(PullConsumerListener.class);
    private final MessagePullListener listener;

    public PullConsumerListener(PullConsumer consumer) {
        this.listener = consumer.getPullListener();
    }

    @Override
    public void onPartitionChanged(ClientChannel channel, PartitionChangedSignal signal) {

    }

    @Override
    public void onNodeOffline(ClientChannel channel, NodeOfflineSignal signal) {

    }

    @Override
    public void onPullMessage(String topic, String queue, int ledger, int limit, int epoch, long index, ByteBuf data) {
        List<Message> messages = new ArrayList<>(limit);
        try {
            for (int i = 0; i < limit; i++) {
                int messageLength = data.readInt();

                int queueLength = data.readInt();
                data.skipBytes(queueLength);

                int messageEpoch = data.readInt();
                long position = data.readLong();

                ByteBuf buf = data.retainedSlice(data.readerIndex(), messageLength - 16 - queueLength);

                SendMessageExtras extras = readProto(buf, SendMessageExtras.parser());
                byte[] body = ByteBufUtil.buf2Bytes(buf.readBytes(buf.readableBytes()));

                messages.add(new Message(topic, queue, body, messageEpoch, position, new Message.Extras(extras.getExtrasMap())));
            }
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error(t.getMessage(), t);
            }
        } finally {
            ByteBufUtil.release(data);
        }

        PullResult result = new PullResult(ledger, topic, queue, limit, epoch, index, messages);
        listener.onMessage(result);
    }
}
