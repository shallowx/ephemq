package org.leopard.dispatch;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Promise;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectCollection;
import org.leopard.client.consumer.Subscription;
import org.leopard.common.logging.InternalLogger;
import org.leopard.common.logging.InternalLoggerFactory;
import org.leopard.internal.config.BrokerConfig;
import org.leopard.ledger.Cursor;
import org.leopard.ledger.Offset;
import org.leopard.ledger.Storage;
import org.leopard.remote.Type;
import org.leopard.remote.codec.MessagePacket;
import org.leopard.remote.proto.notify.MessagePushSignal;
import org.leopard.remote.util.ByteBufUtils;
import org.leopard.remote.util.ProtoBufUtils;

import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.leopard.remote.processor.ProcessCommand.Client.HANDLE_MESSAGE;
import static org.leopard.remote.util.NetworkUtils.newImmediatePromise;

@SuppressWarnings("all")
@ThreadSafe
public class DefaultEntryDispatchProcessor implements DispatchProcessor {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(DefaultEntryDispatchProcessor.class);

    private final int ledgerId;
    private final int subscribeLimit;
    private final int handleLimit;
    private final int assignLimit;
    private final int alignLimit;

    private final EntryDispatchHelper helper;
    private final Storage storage;
    private final List<EntryEventExecutorHandler> handlers = new ArrayList<>();

    public DefaultEntryDispatchProcessor(int ledgerId, BrokerConfig config, Storage storage) {
        this.storage = storage;
        Offset currentOffset = storage.currentOffset();

        this.ledgerId = ledgerId;
        this.subscribeLimit = config.getIoThreadLimit();
        this.handleLimit = config.getMessagePushHandleLimit();
        this.assignLimit = config.getMessagePushHandleAssignLimit();
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
            EntryEventExecutorHandler handler = helper.accessCheckedHandler(channel, subscribeLimit);
            ConcurrentMap<Channel, EntrySubscription> channelShips = handler.getChannelShips();
            EntrySubscription oldSubscription = channelShips.get(channel);

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

                        EntryAttributes attributes = EntryAttributes
                                .newBuilder()
                                .offset(dispatchOffset)
                                .cursor(handler.getNextCursor())
                                .assignLimit(assignLimit)
                                .alignLimit(alignLimit)
                                .subscription(newSubscription)
                                .build();
                        EntryDelayTaskAssignor task = new EntryDelayTaskAssignor(attributes, helper, ledgerId);
                        task.assign();
                    } else {
                        dispatchOffset = offset;
                        newSubscription.setOffset(dispatchOffset);
                    }
                } else {
                    dispatchOffset = storage.currentOffset();
                    newSubscription.setOffset(dispatchOffset);
                }

                channelShips.put(channel, newSubscription);
                Object2ObjectMap<String, EntrySubscription> subscribeShips = handler.getSubscribeShips();
                subscribeShips.put(topic + ":" + queue, newSubscription);
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
            if (logger.isDebugEnabled()) {
                logger.debug("Channel<{}> subscribe failure, topic={} queue={} version={} offset={}", channel, topic, queue, version, offset);
            }
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
            EntryEventExecutorHandler handler = helper.getHandler(channel);
            if (handler == null) {
                promise.trySuccess(null);
                return;
            }

            ConcurrentMap<Channel, EntrySubscription> subscriptionShips = handler.getChannelShips();
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

                Object2ObjectMap<String, EntrySubscription> subscribeShips = handler.getSubscribeShips();
                subscribeShips.remove(topic + ":" + queue);

                promise.trySuccess(null);
            });
        } catch (Throwable t) {
            if (logger.isDebugEnabled()) {
                logger.debug("Channel<{}> clean subscribe failure, topic={} queue={} version={} offset={}", channel, topic, queue);
            }
            promise.tryFailure(t);
        }
    }

    @Override
    public void clearChannel(Channel channel) {
        helper.remove(channel);
    }

    @Override
    public void handleRequest(String topic) {
        if (handlers.isEmpty()) {
            return;
        }

        for (EntryEventExecutorHandler handler : handlers) {
            Cursor nextCursor = handler.getNextCursor();
            if (nextCursor != null) {
                doHandle(handler);
            }
        }
    }

    private void doHandle(EntryEventExecutorHandler handler) {
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

    private void dispatch(EntryEventExecutorHandler handler) {
        Cursor cursor = handler.getNextCursor();
        if (cursor == null) {
            handler.getTriggered().set(false);
            return;
        }

        Collection<EntrySubscription> channelShips = handler.getChannelShips().values();

        if (channelShips.isEmpty()) {
            handler.getTriggered().set(false);
            return;
        }

        Object2ObjectMap<String, EntrySubscription> subscribeShips = handler.getSubscribeShips();
        if (subscribeShips.isEmpty()) {
            handler.getTriggered().set(false);
            return;
        }

        ObjectCollection<EntrySubscription> entrySubscriptions = subscribeShips.values();

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
                    topic = ByteBufUtils.buf2String(topicBuf, topicLength);

                    payload.skipBytes(topicLength);

                    int queueLength = payload.readInt();
                    queueBuf = payload.retainedSlice(payload.readerIndex(), queueLength);
                    queue = ByteBufUtils.buf2String(queueBuf, queueLength);

                    EntrySubscription entrySubscription = subscribeShips.get(topic + ":" + queue);
                    if (entrySubscription == null) {
                        if (whole > handleLimit) {
                            break;
                        }
                        continue;
                    }

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

                    Channel channel = entrySubscription.getChannel();
                    if (!channel.isActive()) {
                        if (logger.isWarnEnabled()) {
                            logger.warn("Channel<{}> is not active, and will remove it", channel.toString());
                        }
                        continue;
                    }

                    short subscriptionVersion = entrySubscription.getVersion();
                    if (subscriptionVersion != -1 && subscriptionVersion != version) {
                        continue;
                    }

                    entrySubscription.setOffset(messageOffset);
                    message = buildByteBuf(topic, queue, version, new Offset(epoch, index), payload, channel.alloc());

                    if (!channel.isWritable()) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Channel<{}> is not allowed writable, transfer to pursue", channel.toString());
                        }

                        EntryAttributes attributes = EntryAttributes
                                .newBuilder()
                                .offset(messageOffset)
                                .assignLimit(assignLimit)
                                .alignLimit(alignLimit)
                                .cursor(cursor.clone())
                                .subscription(entrySubscription)
                                .build();
                        EntryDelayTaskAssignor task = new EntryDelayTaskAssignor(attributes, helper, ledgerId);
                        channel.writeAndFlush(message.retainedSlice(), task.newPromise());
                        return;
                    }
                    channel.writeAndFlush(message.retainedSlice());

                    if (whole > handleLimit) {
                        break;
                    }
                } catch (Throwable t) {
                    if (logger.isErrorEnabled()) {
                        logger.error("Channel push message failed, topic={} queue={} offset={} error:{}", topic, queue, nextOffset, t);
                    }
                } finally {
                    ByteBufUtils.release(message);
                    ByteBufUtils.release(payload);
                    ByteBufUtils.release(queueBuf);
                    ByteBufUtils.release(topicBuf);
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

    private ByteBuf buildByteBuf(String topic, String queue, short version, Offset offset, ByteBuf payload, ByteBufAllocator alloc) {
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

            int signalLength = ProtoBufUtils.protoLength(signal);
            int payloadLength = payload.readableBytes();

            buf = alloc.ioBuffer(MessagePacket.HEADER_LENGTH + signalLength);

            buf.writeByte(MessagePacket.MAGIC_NUMBER);
            buf.writeMedium(MessagePacket.HEADER_LENGTH + signalLength + payloadLength);
            buf.writeShort(version);
            buf.writeByte(HANDLE_MESSAGE);
            buf.writeByte(Type.PUSH.sequence());
            buf.writeInt(0);

            ProtoBufUtils.writeProto(buf, signal);

            buf = Unpooled.wrappedUnmodifiableBuffer(buf, payload.retainedSlice(payload.readerIndex(), payloadLength));

            return buf;
        } catch (Throwable t) {
            ByteBufUtils.release(buf);
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
