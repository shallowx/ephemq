package org.shallow.log.handle.push;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.shallow.processor.ProcessCommand.Client.HANDLE_MESSAGE;

@SuppressWarnings("all")
@ThreadSafe
public class EntryPushDispatcher implements PushDispatcher {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(EntryPushDispatcher.class);

    private final int ledgerId;
    private final int subscribeLimit;
    private final EntryHandleHelper helper;
    private final Storage storage;
    private final List<EntryPushHandler> handlers = new ArrayList<>();

    @SuppressWarnings("unused")
    public EntryPushDispatcher(int ledgerId, BrokerConfig config, Storage storage) {
        this.storage = storage;
        Offset currentOffset = storage.currentOffset();
        this.ledgerId = ledgerId;
        this.subscribeLimit = config.getIoThreadLimit();
        this.helper = new EntryHandleHelper(config);
    }

    @Override
    public void subscribe(Channel channel, String queue, Offset offset, short version) {
        if (!channel.isActive() || !channel.isWritable()) {
            if (logger.isWarnEnabled()) {
                logger.warn("Channel<{}> is not active for subscribe", channel.toString());
            }
        }

        try {
            EventExecutor executor = helper.channelExecutor(channel);
           if (executor.inEventLoop()) {
                doSubscribe(channel, queue, offset, version);
           } else {
                executor.execute(() -> doSubscribe(channel, queue, offset, version));
           }
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error(t.getMessage(), t);
            }
        }
    }

    private void doSubscribe(Channel channel, String queue, Offset offset, short version) {
        EntryPushHandler handler = helper.applyHandler(channel, subscribeLimit);
        ConcurrentMap<Channel, Subscription> subscriptionShips = handler.getSubscriptionShips();
        Subscription oldSubscription = subscriptionShips.get(channel);

        List<String> queues = new ArrayList<>();
        if (oldSubscription != null) {
            List<String> oldQueues = oldSubscription.getQueue();
            queues.addAll(oldQueues);
        }
        queues.add(queue);

        Subscription newSubscription = Subscription
                .newBuilder()
                .channel(channel)
                .handler(handler)
                .offset(offset)
                .queue(queues)
                .version(version)
                .build();

        EventExecutor dispatchExecutor = handler.getDispatchExecutor();
        dispatchExecutor.execute(() -> {
            if (handler.getNextCursor() == null) {

                Offset currentOffset = storage.currentOffset();
                handler.setNextOffset(currentOffset);
                handler.setNextCursor(storage.locateCursor(currentOffset));

                handlers.add(handler);
            }

            Offset dispatchOffset;
            if (offset != null) {
                Offset earlyOffset = storage.headSegment().headOffset();
                if (earlyOffset.after(offset)) {
                    dispatchOffset = earlyOffset;
                    if (logger.isDebugEnabled()) {
                        logger.debug("Subscribe offset is expired, and will purse from commit log file, offset={} earlyOffset={}", offset, earlyOffset);
                    }
                    // TODO submit purse from commit log file
                } else {
                    dispatchOffset = offset;
                    newSubscription.setOffset(dispatchOffset);
                }
            } else {
                dispatchOffset = storage.currentOffset();
                newSubscription.setOffset(dispatchOffset);
            }

            subscriptionShips.put(channel, newSubscription);
            helper.putHandler(channel, handler);
        });
    }

    @Override
    public void clean(Channel channel, String queue) {
        try {
            EventExecutor executor = helper.channelExecutor(channel);
            if (executor.inEventLoop()) {
                doClean(channel, queue);
            } else {
                executor.execute(() -> doClean(channel, queue));
            }
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error(t.getMessage(), t);
            }
        }
    }

    private void doClean(Channel channel, String queue) {
        EntryPushHandler handler = helper.getHandler(channel);
        if (handler == null) {
            return;
        }

        ConcurrentMap<Channel, Subscription> subscriptionShips = handler.getSubscriptionShips();
        Subscription subscription = subscriptionShips.get(channel);
        if (subscription == null) {
            return;
        }

        EventExecutor dispatchExecutor = handler.getDispatchExecutor();
        dispatchExecutor.execute(() -> {
            List<String> oldQueues = subscription.getQueue();
            oldQueues.remove(queue);

            if (oldQueues.isEmpty()) {
                handler.setNextCursor(null);
                handler.setNextOffset(null);

                subscriptionShips.remove(channel);
            }
        });
    }

    @Override
    public void handle(String topic) {
        if (handlers.isEmpty()) {
            return;
        }

        for (EntryPushHandler handler : handlers) {
            Cursor nextCursor = handler.getNextCursor();
            if (nextCursor != null) {
                doHandle(topic, handler);
            }
        }
    }

    private void doHandle(String topic, EntryPushHandler handler) {
        AtomicBoolean triggered = handler.getTriggered();
        if (triggered.compareAndSet(false, true)) {
            try {
                handler.getDispatchExecutor().execute(() -> dispatch(topic, handler));
            } catch (Throwable t) {
                if (logger.isErrorEnabled()) {
                    logger.error("Entry push handler submit failure");
                }
            }
        }
    }

    private void dispatch(String topic, EntryPushHandler handler) {
        Cursor cursor = handler.getNextCursor();
        if (cursor == null) {
            handler.getTriggered().set(false);
            return;
        }

        Collection<Subscription> subscriptionShips = handler.getSubscriptionShips().values();

        if (subscriptionShips.isEmpty()) {
            handler.getTriggered().set(false);
            return;
        }

        Offset nextOffset = handler.getNextOffset();
        try {
            ByteBuf payload;
            while ((payload = cursor.next()) != null) {
                ByteBuf message = null;
                ByteBuf queueBuf = null;
                String queue = null;
                try {
                    short version = payload.readShort();

                    int queueLength = payload.readInt();
                    queueBuf = payload.retainedSlice(payload.readerIndex(), queueLength);
                    queue = ByteBufUtil.buf2String(queueBuf, queueLength);

                    payload.skipBytes(queueLength);
                    int epoch = payload.readInt();
                    long index = payload.readLong();

                    Offset messageOffset = buildOffset(epoch, index);
                    nextOffset = messageOffset;

                    if (messageOffset.before(nextOffset)) {
                        continue;
                    }

                    for (Subscription subscription : subscriptionShips) {
                        Channel channel = subscription.getChannel();
                        if (!channel.isActive()) {
                            if (logger.isWarnEnabled()) {
                                logger.warn("Channel<{}> is not active, and will remove it", channel.toString());
                            }
                            continue;
                        }

                        subscription.setOffset(messageOffset);

                        short subscriptionVersion = subscription.getVersion();
                        if (subscriptionVersion != -1 && subscriptionVersion != version) {
                            continue;
                        }

                        if (!subscription.getQueue().contains(queue)) {
                            continue;
                        }

                        message = buildByteBuf(topic, queue, version, new Offset(epoch, index), payload, channel.alloc());

                        if (!channel.isWritable()) {
                            if (logger.isDebugEnabled()) {
                                logger.debug("Channel<{}> is not allowed writable, transfer to pursue", channel.toString());
                            }
                            // TODO
                        }
                        channel.writeAndFlush(message.retainedSlice());
                     }
                } catch (Throwable t){
                    if (logger.isErrorEnabled()) {
                        logger.error("Channel push message failed, topic={} queue={} offset={} error:{}", topic, queue, nextOffset, t);
                    }
                } finally {
                    ByteBufUtil.release(message);
                    ByteBufUtil.release(payload);
                    ByteBufUtil.release(queueBuf);
                }
            }
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error(t.getMessage(), t);
            }
        } finally {
            handler.getTriggered().set(false);
        }

        handler.setNextOffset(nextOffset);
        if (cursor.hashNext()) {
            doHandle(topic, handler);
        }
    }

    private ByteBuf buildByteBuf(String topic, String queue, short version, Offset offset , ByteBuf payload, ByteBufAllocator alloc) {
        ByteBuf buf = null;
        try {
            MessagePushSignal signal = MessagePushSignal
                    .newBuilder()
                    .setQueue(queue)
                    .setTopic(topic)
                    .setLedgerId(ledgerId)
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

    private Offset buildOffset(int epoch, long index) {
        return new Offset(epoch, index);
    }

    @Override
    public void shutdownGracefully() {

    }
}
