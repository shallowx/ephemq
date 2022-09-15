package org.shallow.handle;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Promise;
import org.shallow.Type;
import org.shallow.codec.MessagePacket;
import org.shallow.consumer.push.Subscription;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.log.Cursor;
import org.shallow.log.Offset;
import org.shallow.log.Storage;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.proto.notify.MessagePushSignal;
import org.shallow.util.ByteBufUtil;
import org.shallow.util.NetworkUtil;
import org.shallow.util.ProtoBufUtil;

import javax.annotation.concurrent.ThreadSafe;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.shallow.processor.ProcessCommand.Client.HANDLE_MESSAGE;
import static org.shallow.util.NetworkUtil.newImmediatePromise;

@SuppressWarnings("all")
@ThreadSafe
public class DefaultEntryPushDispatcher implements PushDispatchProcessor {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(DefaultEntryPushDispatcher.class);

    private final int ledgerId;
    private final int subscribeLimit;
    private final int handleLimit;
    private final int traceLimit;
    private final int alignLimit;

    private final EntryDispatchHelper helper;
    private final Storage storage;
    private final List<EntryPushHandler> handlers = new ArrayList<>();

    @SuppressWarnings("unused")
    public DefaultEntryPushDispatcher(int ledgerId, BrokerConfig config, Storage storage) {
        this.storage = storage;
        Offset currentOffset = storage.currentOffset();
        this.ledgerId = ledgerId;
        this.subscribeLimit = config.getIoThreadLimit();
        this.handleLimit = config.getMessagePushHandleLimit();
        this.traceLimit = config.getMessagePushHandleTraceLimit();
        this.alignLimit = config.getMessagePushHandleAlignLimit();
        this.helper = new EntryDispatchHelper(config);
    }

    @Override
    public void subscribe(Channel channel, String topic, String queue, Offset offset, short version, Promise<Subscription> subscribePromise) {
        if (!channel.isActive()) {
            if (logger.isWarnEnabled()) {
                logger.warn("Channel<{}> is not active for subscribe", channel.toString());
            }
            subscribePromise.tryFailure(new RuntimeException(String.format("Channel<{}> is not active for subscribe", channel.toString())));
            return;
        }

        try {
            EventExecutor executor = helper.channelExecutor(channel);
           if (executor.inEventLoop()) {
                doSubscribe(channel, topic, queue, offset, version, subscribePromise);
           } else {
                executor.execute(() -> doSubscribe(channel, topic, queue, offset, version, subscribePromise));
           }
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error(t.getMessage(), t);
            }
            subscribePromise.tryFailure(t);
        }
    }

    private void doSubscribe(Channel channel, String topic, String queue, Offset offset, short version, Promise<Subscription> subscribePromise) {
        try {
            EntryPushHandler handler = helper.applyHandler(channel, subscribeLimit);
            ConcurrentMap<Channel, EntrySubscription> subscriptionShips = handler.getSubscriptionShips();
            EntrySubscription oldSubscription = subscriptionShips.get(channel);

            List<String> queues = new ArrayList<>();
            if (oldSubscription != null) {
                List<String> oldQueues = oldSubscription.getQueue();
                queues.addAll(oldQueues);
            }
            queues.add(queue);

            EntrySubscription newSubscription = EntrySubscription
                    .newBuilder()
                    .channel(channel)
                    .handler(handler)
                    .topic(topic)
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

                        EntryTrace trace = EntryTrace
                                .newBuilder()
                                .offset(dispatchOffset)
                                .cursor(handler.getNextCursor())
                                .traceLimit(traceLimit)
                                .alignLimit(alignLimit)
                                .subscription(newSubscription)
                                .build();
                        EntryTraceDelayTask task = new EntryTraceDelayTask(trace, helper, ledgerId);
                        task.trace();
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

                subscribePromise.trySuccess(Subscription
                        .newBuilder()
                                .epoch(dispatchOffset.epoch())
                                .index(dispatchOffset.index())
                                .version(version)
                                .queue(queue)
                        .build());
            });
        } catch (Throwable t) {
            subscribePromise.tryFailure(t);
        }
    }

    @Override
    public void clean(Channel channel, String topic, String queue, Promise<Void> promise) {
        try {
            EventExecutor executor = helper.channelExecutor(channel);
            if (executor.inEventLoop()) {
                doClean(channel, topic, queue, promise);
            } else {
                executor.execute(() -> doClean(channel, topic, queue, promise));
            }
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error(t.getMessage(), t);
            }
        }
    }

    private void doClean(Channel channel, String topic, String queue, Promise<Void> promise) {
        try {
            EntryPushHandler handler = helper.getHandler(channel);
            if (handler == null) {
                promise.trySuccess(null);
                return;
            }

            ConcurrentMap<Channel, EntrySubscription> subscriptionShips = handler.getSubscriptionShips();
            EntrySubscription subscription = subscriptionShips.get(channel);
            if (subscription == null) {
                promise.trySuccess(null);
                return;
            }

            EventExecutor dispatchExecutor = handler.getDispatchExecutor();
            dispatchExecutor.execute(() -> {
                List<String> oldQueues = subscription.getQueue();
                if (subscription.getTopic().equals(topic)) {
                    oldQueues.remove(queue);
                }

                if (oldQueues.isEmpty()) {
                    handler.setNextCursor(null);
                    handler.setNextOffset(null);

                    subscriptionShips.remove(channel);
                }

                promise.trySuccess(null);
            });
        } catch (Throwable t) {
            promise.tryFailure(t);
        }
    }

    @Override
    public void clearChannel(Channel channel) {
        helper.remove(channel);
    }

    @Override
    public void handle(String topic) {
        if (handlers.isEmpty()) {
            return;
        }

        for (EntryPushHandler handler : handlers) {
            Cursor nextCursor = handler.getNextCursor();
            if (nextCursor != null) {
                doHandle(handler);
            }
        }
    }

    private void doHandle(EntryPushHandler handler) {
        AtomicBoolean triggered = handler.getTriggered();
        if (triggered.compareAndSet(false, true)) {
            try {
                handler.getDispatchExecutor().execute(() -> dispatch(handler));
            } catch (Throwable t) {
                if (logger.isErrorEnabled()) {
                    logger.error("Entry push handler submit failure");
                }
            }
        }
    }

    private void dispatch(EntryPushHandler handler) {
        Cursor cursor = handler.getNextCursor();
        if (cursor == null) {
            handler.getTriggered().set(false);
            return;
        }

        Collection<EntrySubscription> subscriptionShips = handler.getSubscriptionShips().values();

        if (subscriptionShips.isEmpty()) {
            handler.getTriggered().set(false);
            return;
        }

        Offset nextOffset = handler.getNextOffset();
        try {
            ByteBuf payload;
            int whole = 0;
            while ((payload = cursor.next()) != null) {
                ByteBuf message = null;

                ByteBuf queueBuf = null;
                String queue = null;

                ByteBuf topicBuf = null;
                String topic = null;

                whole++;
                try {
                    short version = payload.readShort();

                    int topicLength = payload.readInt();
                    topicBuf = payload.retainedSlice(payload.readerIndex(), topicLength);
                    topic = ByteBufUtil.buf2String(topicBuf, topicLength);

                    payload.skipBytes(topicLength);

                    int queueLength = payload.readInt();
                    queueBuf = payload.retainedSlice(payload.readerIndex(), queueLength);
                    queue = ByteBufUtil.buf2String(queueBuf, queueLength);

                    payload.skipBytes(queueLength);

                    int epoch = payload.readInt();
                    long index = payload.readLong();

                    Offset messageOffset = buildOffset(epoch, index);
                    if (!messageOffset.after(nextOffset)) {
                        if (whole > handleLimit) {
                            break;
                        }
                        continue;
                    }

                    nextOffset = messageOffset;

                    for (EntrySubscription subscription : subscriptionShips) {
                        if (subscription == null) {
                            if (whole > handleLimit) {
                                break;
                            }
                            continue;
                        }

                        Channel channel = subscription.getChannel();
                        if (!channel.isActive()) {
                            if (logger.isWarnEnabled()) {
                                logger.warn("Channel<{}> is not active, and will remove it", channel.toString());
                            }
                            continue;
                        }

                        short subscriptionVersion = subscription.getVersion();
                        if (subscriptionVersion != -1 && subscriptionVersion != version) {
                            continue;
                        }

                        if (!subscription.getQueue().contains(queue)) {
                            continue;
                        }

                        if (!subscription.getTopic().equals(topic)) {
                            continue;
                        }

                        subscription.setOffset(messageOffset);
                        message = buildByteBuf(topic, queue, version, new Offset(epoch, index), payload, channel.alloc());

                        if (!channel.isWritable()) {
                            if (logger.isDebugEnabled()) {
                                logger.debug("Channel<{}> is not allowed writable, transfer to pursue", channel.toString());
                            }

                            EntryTrace trace = EntryTrace
                                    .newBuilder()
                                    .offset(messageOffset)
                                    .traceLimit(traceLimit)
                                    .alignLimit(alignLimit)
                                    .cursor(cursor.copy())
                                    .subscription(subscription)
                                    .build();
                            EntryTraceDelayTask task = new EntryTraceDelayTask(trace, helper, ledgerId);
                            channel.writeAndFlush(message.retainedSlice(), task.newPromise());
                            return;
                        }
                        channel.writeAndFlush(message.retainedSlice());
                     }

                    if (whole > handleLimit) {
                        break;
                    }
                } catch (Throwable t){
                    if (logger.isErrorEnabled()) {
                        logger.error("Channel push message failed, topic={} queue={} offset={} error:{}", topic, queue, nextOffset, t);
                    }
                } finally {
                    ByteBufUtil.release(message);
                    ByteBufUtil.release(payload);
                    ByteBufUtil.release(queueBuf);
                    ByteBufUtil.release(topicBuf);
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
            doHandle(handler);
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
        if (handlers.isEmpty()) {
            return;
        }

        if (logger.isInfoEnabled()) {
            logger.info("Entry push dispatcher will close");
        }

        helper.close(new EntryDispatchHelper.CloseFunction<Channel, String, String>() {
            @Override
            public void consume(Channel channel, String topic, String queue) {
                doClean(channel, topic, queue, newImmediatePromise());
            }
        });

        handlers.clear();
    }
}
