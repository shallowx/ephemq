package org.shallow.log.handle.push;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import org.shallow.Type;
import org.shallow.codec.MessagePacket;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.log.Cursor;
import org.shallow.log.Offset;
import org.shallow.log.Storage;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.proto.notify.MessagePushSignal;
import org.shallow.util.ByteBufUtil;
import org.shallow.util.ProtoBufUtil;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import static org.shallow.processor.ProcessCommand.Client.HANDLE_MESSAGE;
import static org.shallow.util.ObjectUtil.isNotNull;
import static org.shallow.util.ObjectUtil.isNull;

@ThreadSafe
public class EntryPushDispatcher {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(EntryPushDispatcher.class);

    private final BrokerConfig config;
    private final Storage storage;
    private Cursor cursor;
    private final Map<String, Set<Subscription>> subscriptionMap = new ConcurrentHashMap<>();

    public EntryPushDispatcher(BrokerConfig config, Storage storage) {
        this.config = config;
        this.storage = storage;

        Offset currentOffset = storage.currentOffset();
        this.cursor = storage.locateCursor(currentOffset);
    }

    public void subscribe(Channel channel, String queue, Offset offset, short version) {
        if (!channel.isActive() || !channel.isWritable()) {
            if (logger.isWarnEnabled()) {
                logger.warn("chanel is not active for subscribe");
            }
        }
        subscriptionMap.computeIfAbsent(queue, k -> new CopyOnWriteArraySet<>()).add(new Subscription(channel, queue, offset, version));
    }

    public void clean(Channel channel, String queue) {
        if (subscriptionMap.isEmpty()) {
            return;
        }

        Set<Subscription> subscriptions = subscriptionMap.get(queue);
        if (subscriptions.isEmpty()) {
            subscriptionMap.remove(queue);
            return;
        }

        Subscription subscription = subscriptions.stream().filter(k -> k.channel().equals(channel)).findFirst().orElse(null);
        subscriptions.remove(subscription);
    }

    public void handle(String topic){
        doHandle(topic);
    }

    private void doHandle(String topic) {
        ByteBuf payload;
        while (isNotNull((payload = cursor.next()))) {
            ByteBuf message = null;
            try {
                short version = payload.readShort();
                int queueLength = payload.readInt();
                String queue = ByteBufUtil.buf2String(payload.retainedSlice(payload.readerIndex(), queueLength), queueLength);

                Set<Subscription> subscriptions = subscriptionMap.get(queue);
                if (isNull(subscriptions) || subscriptions.isEmpty()) {
                    return;
                }

                payload.skipBytes(queueLength);

                int epoch = payload.readInt();
                long index = payload.readLong();

                // TODO optimization
                for (Subscription subscription : subscriptions) {
                    Offset theOffset = new Offset(epoch, index);
                    Offset subscribeOffset = subscription.offset();
                    if (theOffset.before(subscribeOffset)) {
                        continue;
                    }

                    Channel channel = subscription.channel();
                    message = buildByteBuf(topic, queue, version, new Offset(epoch, index), payload, channel.alloc());

                    if (!subscription.queue().equals(queue) || subscription.version() != version) {
                        continue;
                    }

                    if (!channel.isActive()) {
                        if (logger.isWarnEnabled()) {
                            logger.warn("Channel is not active, and will remove it");
                        }
                        continue;
                    }

                    if (!channel.isWritable()) {
                        continue;
                    }

                    channel.writeAndFlush(message.retainedSlice());
                }
            } catch (Throwable t) {
                if (logger.isErrorEnabled()) {
                    logger.error(t.getMessage(), t);
                }
            } finally {
                ByteBufUtil.release(message);
                ByteBufUtil.release(payload);
            }
        }
    }

    private ByteBuf buildByteBuf(String topic, String queue, short version, Offset offset , ByteBuf payload, ByteBufAllocator alloc) {
        ByteBuf buf = null;
        try {
            MessagePushSignal signal = MessagePushSignal
                    .newBuilder()
                    .setQueue(queue)
                    .setTopic(topic)
                    .setEpoch(offset.epoch())
                    .setIndex(offset.index())
                    .build();

            int signalLength = ProtoBufUtil.protoLength(signal);
            int payloadLength = payload.readableBytes();

            buf = alloc.ioBuffer(MessagePacket.HEADER_LENGTH + signalLength);

            buf.writeByte(MessagePacket.MAGIC_NUMBER);
            buf.writeMedium(MessagePacket.HEADER_LENGTH + signalLength + payloadLength);
            buf.writeShort(version);
            buf.writeByte(HANDLE_MESSAGE);
            buf.writeByte(Type.PUSH.sequence());
            buf.writeInt(0);

            ProtoBufUtil.writeProto(buf, signal);

            buf = Unpooled.wrappedUnmodifiableBuffer(buf, payload.retainedSlice(payload.readerIndex(), payloadLength));

            return buf;
        } catch (Throwable t) {
            ByteBufUtil.release(buf);
            throw new RuntimeException(String.format("Failed to build payload: queue=%s", queue), t);
        }
    }

    public void shutdownGracefully() {
        subscriptionMap.clear();
    }
}
