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

    private MessagePullListener listener;

    public PullConsumerListener() {
    }

    public void registerListener(MessagePullListener listener) {
        this.listener = listener;
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
        if (data.readerIndex() >= data.writerIndex()) {
            if (logger.isWarnEnabled()) {
                logger.warn("Message pull result is empty, and topic={} queue={} ledger={} limit= {} epoch={} index={}", topic, queue, ledger, limit, epoch, index);
            }
            listener.onMessage(null);
            return;
        }

        try {
            int skipBytes = 0;
            for (int i = 0; i < limit; i++) {
                if (skipBytes != 0) {
                    data.skipBytes(skipBytes);
                }

                if (data.readerIndex() >= data.writerIndex()) {
                    break;
                }

                int messageLength = data.readInt();
                short theVersion = data.readShort();
                int queueLength = data.readInt();
                data.skipBytes(queueLength);

                int messageEpoch = data.readInt();
                long position = data.readLong();

                int sliceLength = messageLength - 18 - queueLength;
                ByteBuf buf = data.retainedSlice(data.readerIndex(), sliceLength);

                SendMessageExtras extras = readProto(buf, SendMessageExtras.parser());
                byte[] body = ByteBufUtil.buf2Bytes(buf.readBytes(buf.readableBytes()));

                messages.add(new Message(topic, queue, theVersion, body, messageEpoch, position, new Message.Extras(extras.getExtrasMap())));
                skipBytes = sliceLength;
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
