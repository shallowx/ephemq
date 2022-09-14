package org.shallow.consumer.pull;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.shallow.Extras;
import org.shallow.Message;
import org.shallow.consumer.ConsumeListener;
import org.shallow.consumer.MessagePostInterceptor;
import org.shallow.internal.Listener;
import org.shallow.internal.ClientChannel;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.proto.notify.NodeOfflineSignal;
import org.shallow.proto.notify.PartitionChangedSignal;
import org.shallow.proto.server.SendMessageExtras;
import org.shallow.util.ByteBufUtil;

import javax.annotation.concurrent.Immutable;
import java.util.ArrayList;
import java.util.List;

import static org.shallow.util.ProtoBufUtil.readProto;

@Immutable
final class PullConsumerListener implements Listener {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(PullConsumerListener.class);

    private MessagePullListener listener;
    private MessagePostInterceptor mpInterceptor;

    public PullConsumerListener() {
    }

    @Override
    public void registerListener(ConsumeListener listener) {
        this.listener = (MessagePullListener) listener;
    }

    @Override
    public void registerInterceptor(MessagePostInterceptor interceptor) {
        this.mpInterceptor = interceptor;
    }

    @Override
    public void onPartitionChanged(ClientChannel channel, PartitionChangedSignal signal) {
        if (logger.isDebugEnabled()) {
            logger.debug("Receive partition changed signal, channel={} signal={}", channel.toString(), signal.toString());
        }
        // do nothing
    }

    @Override
    public void onNodeOffline(ClientChannel channel, NodeOfflineSignal signal) {
        if (logger.isDebugEnabled()) {
            logger.debug("Receive node offline signal, channel={} signal={}", channel.toString(), signal.toString());
        }
        // do nothing
    }

    @Override
    public void onPullMessage(Channel channel, int ledgerId, String topic, String queue, int ledger, int limit, int epoch, long index, ByteBuf data) {
        List<Message> messages = new ArrayList<>(limit);
        if (data.readerIndex() >= data.writerIndex()) {
            if (logger.isWarnEnabled()) {
                logger.warn("Message pull result is empty, and channel={} topic={} queue={} ledger={} limit= {} epoch={} index={}", channel.toString(), topic, queue, ledger, limit, epoch, index);
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

                int topicLength = data.readInt();
                data.skipBytes(topicLength);

                int queueLength = data.readInt();
                data.skipBytes(queueLength);

                int messageEpoch = data.readInt();
                long theIndex = data.readLong();

                int sliceLength = messageLength - 22 - queueLength - topicLength;
                ByteBuf buf = data.retainedSlice(data.readerIndex(), sliceLength);

                SendMessageExtras extras = readProto(buf, SendMessageExtras.parser());
                byte[] body = ByteBufUtil.buf2Bytes(buf.readBytes(buf.readableBytes()));
                Message message = new Message(topic, queue, theVersion, body, messageEpoch, theIndex, new Extras(extras.getExtrasMap()));

                if (mpInterceptor != null) {
                    message = mpInterceptor.interceptor(message);
                }

                messages.add(message);

                skipBytes = sliceLength;
            }
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("Failed to handle pull message, channel={} ledgerId={} topic={} queue={} version={} epoch={} index={}, error={}",
                        channel.toString(), ledger, topic, queue, epoch, index, t);
            }
        } finally {
            ByteBufUtil.release(data);
        }

        PullResult result = new PullResult(ledger, topic, queue, limit, epoch, index, messages);
        listener.onMessage(result);
    }
}
