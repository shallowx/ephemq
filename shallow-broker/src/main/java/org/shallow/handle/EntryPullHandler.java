package org.shallow.handle;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import org.shallow.Type;
import org.shallow.codec.MessagePacket;
import org.shallow.log.Offset;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.proto.notify.MessagePullSignal;
import org.shallow.util.ByteBufUtil;
import org.shallow.util.ProtoBufUtil;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import static org.shallow.processor.ProcessCommand.Client.HANDLE_MESSAGE;
import static org.shallow.util.ObjectUtil.isNull;

public class EntryPullHandler {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(EntryPullHandler.class);

    private final Map<String, Set<Channel>> channels = new ConcurrentHashMap<>();

    public void register(String queue, Channel channel) {
        channels.computeIfAbsent(queue, k -> new CopyOnWriteArraySet<>()).add(channel);
    }

    private void cancel(String queue, Channel channel) {
        Set<Channel> sets = channels.get(queue);
        if (sets.isEmpty()) {
            channels.remove(queue);
            return;
        }
        sets.remove(channel);
    }

    public void write2PullChannel(String topic, String queue, int ledgerId, int limit, Offset offset,  ByteBuf payload) {
        try {
            Set<Channel> sets = channels.get(queue);
            if (isNull(sets) || sets.isEmpty()) {
                return;
            }

            for (Channel channel : sets) {
                ByteBuf buf = null;
                try {
                    if (!channel.isActive() && !channel.isWritable()) {
                        if (logger.isWarnEnabled()) {
                            logger.warn("Channel is not active or is not writable");
                        }
                        return;
                    }

                    buf = buildByteBuf(topic, queue, ledgerId, limit, offset, payload, channel.alloc());
                    channel.writeAndFlush(buf.retainedSlice());
                } catch (Throwable t) {
                    if (logger.isErrorEnabled()) {
                        logger.error("Channel pull message failed, error:{}", t);
                    }
                } finally {
                    cancel(queue, channel);
                    ByteBufUtil.release(buf);
                }
            }
        } catch (Throwable t) {
            throw new RuntimeException("Channel pull message failed", t);
        } finally {
            ByteBufUtil.release(payload);
        }
    }

    private ByteBuf buildByteBuf(String topic, String queue, int ledgerId, int limit, Offset offset , ByteBuf payload, ByteBufAllocator alloc) {
        ByteBuf buf = null;
        try {
            MessagePullSignal signal = MessagePullSignal
                    .newBuilder()
                    .setTopic(topic)
                    .setQueue(queue)
                    .setLedger(ledgerId)
                    .setLimit(limit)
                    .setEpoch(offset.epoch())
                    .setIndex(offset.index())
                    .build();

            int signalLength = ProtoBufUtil.protoLength(signal);
            int payloadLength = payload.readableBytes();

            buf = alloc.ioBuffer(MessagePacket.HEADER_LENGTH + signalLength);

            buf.writeByte(MessagePacket.MAGIC_NUMBER);
            buf.writeMedium(MessagePacket.HEADER_LENGTH + signalLength + payloadLength);
            buf.writeShort(0);
            buf.writeByte(HANDLE_MESSAGE);
            buf.writeByte(Type.PULL.sequence());
            buf.writeInt(0);

            ProtoBufUtil.writeProto(buf, signal);
            buf = Unpooled.wrappedUnmodifiableBuffer(buf, payload.retainedSlice());

            return buf;
        } catch (Throwable t) {
            ByteBufUtil.release(buf);
            throw new RuntimeException(String.format("Failed to build payload: topic=%s queue=%s ledger=%d limit=%d", topic, queue, ledgerId, ledgerId), t);
        }
    }
}
