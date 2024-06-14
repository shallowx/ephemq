package org.meteor.dispatch;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.objects.ObjectArraySet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.common.message.Offset;
import org.meteor.common.util.MessageUtil;
import org.meteor.config.DefaultDispatchConfig;
import org.meteor.exception.DefaultDispatchException;
import org.meteor.ledger.LedgerCursor;
import org.meteor.ledger.LedgerStorage;
import org.meteor.remote.codec.MessagePacket;
import org.meteor.remote.invoke.Command;
import org.meteor.remote.proto.client.MessagePushSignal;
import org.meteor.remote.util.ByteBufUtil;
import org.meteor.remote.util.ProtoBufUtil;

public class DefaultDispatcher {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(DefaultDispatcher.class);
    private final int ledger;
    private final String topic;
    private final LedgerStorage storage;
    private final int followLimit;
    private final int pursueLimit;
    private final int alignLimit;
    private final long pursueTimeoutMilliseconds;
    private final int loadLimit;
    private final IntConsumer counter;
    private final EventExecutor[] executors;
    private final List<DefaultHandler> dispatchHandlers = new CopyOnWriteArrayList<>();
    private final WeakHashMap<DefaultHandler, Integer> weakHandlers = new WeakHashMap<>();
    private final ConcurrentMap<Channel, DefaultHandler> channelHandlers = new ConcurrentHashMap<>();
    private final AtomicBoolean state = new AtomicBoolean(true);

    public DefaultDispatcher(int ledger, String topic, LedgerStorage storage, DefaultDispatchConfig config,
                             EventExecutorGroup group, IntConsumer dispatchCounter) {
        this.ledger = ledger;
        this.topic = topic;
        this.storage = storage;
        this.followLimit = config.getDispatchEntryFollowLimit();
        this.pursueLimit = config.getDispatchEntryPursueLimit();
        this.alignLimit = config.getDispatchEntryAlignLimit();
        this.pursueTimeoutMilliseconds = config.getDispatchEntryPursueTimeoutMilliseconds();
        this.loadLimit = config.getDispatchEntryLoadLimit();
        this.counter = dispatchCounter;

        List<EventExecutor> eventExecutors = new ArrayList<>();
        group.forEach(eventExecutors::add);
        Collections.shuffle(eventExecutors);
        this.executors = eventExecutors.toArray(new EventExecutor[0]);
    }

    public int channelCount() {
        return channelHandlers.size();
    }

    private EventExecutor channelExecutor(Channel channel) {
        return executors[(channel.hashCode() & 0x7fffffff) % executors.length];
    }

    public void reset(Channel channel, Offset resetOffset, IntCollection wholeMarkers, Promise<Integer> promise) {
        try {
            EventExecutor executor = channelExecutor(channel);
            if (executor.inEventLoop()) {
                doReset(channel, resetOffset, wholeMarkers, promise);
            } else {
                executor.execute(() -> doReset(channel, resetOffset, wholeMarkers, promise));
            }
        } catch (Throwable t) {
            promise.tryFailure(t);
        }
    }

    public void doReset(Channel channel, Offset resetOffset, IntCollection wholeMarkers, Promise<Integer> promise) {
        try {
            checkActive();
            if (wholeMarkers.isEmpty()) {
                Promise<Boolean> cleanPromise = ImmediateEventExecutor.INSTANCE.newPromise();
                cleanPromise.addListener(future -> {
                    if (future.isSuccess()) {
                        promise.trySuccess(0);
                    } else {
                        promise.tryFailure(future.cause());
                    }
                });
                doClean(channel, cleanPromise);
                return;
            }

            DefaultHandler handler = allocateHandler(channel);
            ConcurrentMap<Channel, DefaultSynchronization> channelSubscriptionMap = handler.getSubscriptionChannels();
            DefaultSynchronization oldSubscription = channelSubscriptionMap.get(channel);
            DefaultSynchronization newSubscription =
                    new DefaultSynchronization(channel, handler, new IntOpenHashSet(wholeMarkers));
            handler.getDispatchExecutor().execute(() -> {
                Int2ObjectMap<Set<DefaultSynchronization>> markerSubscriptionMap = handler.getSubscriptionMarkers();
                if (oldSubscription != null) {
                    oldSubscription.getMarkers().forEach((int marker) -> detachMarker(markerSubscriptionMap, marker, newSubscription));
                }

                wholeMarkers.forEach((int marker) -> attachMarker(markerSubscriptionMap, marker, newSubscription));
                if (handler.getFollowCursor() == null) {
                    handler.setFollowOffset(storage.currentOffset());
                    handler.setFollowCursor(storage.cursor(handler.getFollowOffset()));
                    dispatchHandlers.add(handler);
                    touchDispatch(handler);
                }

                Offset dispatchOffset;
                if (resetOffset != null) {
                    Offset earlyOffset = storage.headOffset();
                    dispatchOffset = earlyOffset.after(resetOffset) ? earlyOffset : resetOffset;
                } else {
                    dispatchOffset = storage.currentOffset();
                }

                newSubscription.setDispatchOffset(dispatchOffset);
                if (dispatchOffset.before(handler.getFollowOffset())) {
                    PursueTask<DefaultSynchronization> task =
                            new PursueTask<>(newSubscription, storage.cursor(dispatchOffset), dispatchOffset);
                    submitPursue(task);
                } else {
                    newSubscription.setFollowed(true);
                }
            });

            channelSubscriptionMap.put(channel, newSubscription);
            channelHandlers.putIfAbsent(channel, handler);
            promise.trySuccess(newSubscription.getMarkers().size());
        } catch (Throwable t) {
            promise.tryFailure(t);
        }
    }

    public void alter(Channel channel, IntCollection appendMarkers, IntCollection deleteMarkers, Promise<Integer> promise) {
        try {
            EventExecutor executor = channelExecutor(channel);
            if (executor.inEventLoop()) {
                doAlter(channel, appendMarkers, deleteMarkers, promise);
            } else {
                executor.execute(() -> doAlter(channel, appendMarkers, deleteMarkers, promise));
            }
        } catch (Throwable t) {
            promise.tryFailure(t);
        }
    }

    private void doAlter(Channel channel, IntCollection appendMarkers, IntCollection deleteMarkers, Promise<Integer> promise) {
        try {
            checkActive();
            DefaultHandler handler = channelHandlers.get(channel);
            ConcurrentMap<Channel, DefaultSynchronization> channelSubscriptionMap =
                    handler == null ? null : handler.getSubscriptionChannels();
            DefaultSynchronization subscription =
                    channelSubscriptionMap == null ? null : channelSubscriptionMap.get(channel);
            if (subscription == null) {
                promise.tryFailure(
                        new DefaultDispatchException(
                                String.format("Channel<%s> alter is invalid", channel.toString())));
                return;
            }

            handler.getDispatchExecutor().execute(() -> {
                Int2ObjectMap<Set<DefaultSynchronization>> markerSubscriptionMap = handler.getSubscriptionMarkers();
                deleteMarkers.forEach((int marker) -> detachMarker(markerSubscriptionMap, marker, subscription));
                appendMarkers.forEach((int marker) -> attachMarker(markerSubscriptionMap, marker, subscription));

                if (markerSubscriptionMap.isEmpty()) {
                    dispatchHandlers.remove(handler);
                    handler.setFollowCursor(null);
                    handler.setFollowOffset(null);
                }
            });

            IntSet markers = subscription.getMarkers();
            markers.removeAll(deleteMarkers);
            markers.addAll(appendMarkers);
            if (markers.isEmpty()) {
                channelSubscriptionMap.remove(channel);
                channelHandlers.remove(channel);
            }
            promise.trySuccess(markers.size());
        } catch (Throwable t) {
            promise.tryFailure(t);
        }
    }

    public void clean(Channel channel, Promise<Boolean> promise) {
        try {
            EventExecutor executor = channelExecutor(channel);
            if (executor.inEventLoop()) {
                doClean(channel, promise);
            } else {
                executor.execute(() -> doClean(channel, promise));
            }
        } catch (Throwable t) {
            promise.tryFailure(t);
        }
    }

    private void doClean(Channel channel, Promise<Boolean> promise) {
        try {
            checkActive();

            DefaultHandler handler = channelHandlers.get(channel);
            ConcurrentMap<Channel, DefaultSynchronization> channelSubscriptionMap =
                    handler == null ? null : handler.getSubscriptionChannels();
            DefaultSynchronization subscription =
                    channelSubscriptionMap == null ? null : channelSubscriptionMap.get(channel);
            if (subscription == null) {
                promise.trySuccess(false);
                return;
            }

            handler.getDispatchExecutor().execute(() -> {
                Int2ObjectMap<Set<DefaultSynchronization>> markerSubscriptionMap = handler.getSubscriptionMarkers();
                subscription.getMarkers().forEach((int marker) -> detachMarker(markerSubscriptionMap, marker, subscription));
                if (markerSubscriptionMap.isEmpty()) {
                    dispatchHandlers.remove(handler);
                    handler.setFollowOffset(null);
                    handler.setFollowCursor(null);
                }
            });

            channelSubscriptionMap.remove(channel);
            channelHandlers.remove(channel);
            promise.trySuccess(true);
        } catch (Throwable t) {
            promise.tryFailure(t);
        }
    }

    private void touchDispatch(DefaultHandler handler) {
        if (handler.getTriggered().compareAndSet(false, true)) {
            try {
                handler.getDispatchExecutor().execute(() -> doDispatch(handler));
            } catch (Throwable t) {
                if (logger.isErrorEnabled()) {
                    logger.error("Dispatch submit failed, handler[{}]", handler, t);
                }
            }
        }
    }

    private void doDispatch(DefaultHandler handler) {
        LedgerCursor cursor = handler.getFollowCursor();
        if (cursor == null) {
            handler.getTriggered().set(false);
            return;
        }

        Int2ObjectMap<Set<DefaultSynchronization>> markerSubscriptionMap = handler.getSubscriptionMarkers();
        Offset lastOffset = handler.getFollowOffset();
        int count = 0;
        try {
            int runTimes = 0;
            ByteBuf entry;
            while ((entry = cursor.next()) != null) {
                runTimes++;
                ByteBuf payload = null;
                try {
                    Offset offset = MessageUtil.getOffset(entry);
                    if (!offset.after(lastOffset)) {
                        if (runTimes > followLimit) {
                            break;
                        }
                        continue;
                    }

                    lastOffset = offset;
                    int marker = MessageUtil.getMarker(entry);
                    Set<DefaultSynchronization> subscriptions = markerSubscriptionMap.get(marker);
                    if (subscriptions == null) {
                        if (runTimes > followLimit) {
                            break;
                        }
                        continue;
                    }

                    for (DefaultSynchronization subscription : subscriptions) {
                        if (!subscription.isFollowed()) {
                            continue;
                        }
                        Channel channel = subscription.getChannel();
                        if (!channel.isActive()) {
                            continue;
                        }

                        if (!offset.after(subscription.getDispatchOffset())) {
                            continue;
                        }

                        subscription.setDispatchOffset(offset);
                        if (payload == null) {
                            payload = createPayload(marker, offset, entry, channel.alloc());
                        }

                        count++;
                        if (channel.isWritable()) {
                            channel.writeAndFlush(payload.retainedSlice(), channel.voidPromise());
                        } else {
                            subscription.setFollowed(false);
                            PursueTask<DefaultSynchronization> task =
                                    new PursueTask<>(subscription, cursor.copy(), offset);
                            channel.writeAndFlush(payload.retainedSlice(), delayPursue(task));
                        }
                    }
                } catch (Throwable t) {
                    if (logger.isErrorEnabled()) {
                        logger.error("Dispatch failed, handler[{}] lastOffset[{}]", handler, lastOffset, t);
                    }
                } finally {
                    ByteBufUtil.release(entry);
                    ByteBufUtil.release(payload);
                }
                if (runTimes > followLimit) {
                    break;
                }
            }
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("Dispatch execute failed, handler[{}] lastOffset[{}]", handler, lastOffset, t);
            }
        } finally {
            handler.getTriggered().set(false);
        }

        handler.setFollowOffset(lastOffset);
        compute(count);
        if (cursor.hashNext()) {
            touchDispatch(handler);
        }
    }

    private void compute(int count) {
        if (count > 0) {
            try {
                counter.accept(count);
            } catch (Throwable t) {
                if (logger.isErrorEnabled()) {
                    logger.error("Record count failed, ledger[{}] topic[{}]", ledger, topic, t);
                }
            }
        }
    }

    private ChannelPromise delayPursue(PursueTask<DefaultSynchronization> task) {
        ChannelPromise promise = task.getSubscription().getChannel().newPromise();
        promise.addListener((ChannelFutureListener) f -> {
            if (f.channel().isActive()) {
                submitPursue(task);
            }
        });
        return promise;
    }

    private void submitPursue(PursueTask<DefaultSynchronization> task) {
        try {
            channelExecutor(task.getSubscription().getChannel()).execute(() -> {
                doPursue(task);
            });
        } catch (Throwable t) {
            task.getSubscription().setFollowed(true);
            if (logger.isErrorEnabled()) {
                logger.error("Submit failed, task[{}]", task);
            }
        }
    }

    private void doPursue(PursueTask<DefaultSynchronization> task) {
        DefaultSynchronization subscription = task.getSubscription();
        Channel channel = subscription.getChannel();
        AbstractHandler<DefaultSynchronization, DefaultHandler> handler = subscription.getHandler();

        if (!channel.isActive() || subscription != handler.getSubscriptionChannels().get(channel)) {
            return;
        }

        if (System.currentTimeMillis() - task.getPursueTimeMillis() > pursueTimeoutMilliseconds) {
            if (logger.isWarnEnabled()) {
                logger.warn("Giving up pursue task[{}]", task);
            }
            submitFollow(task);
            return;
        }

        IntSet markers = subscription.getMarkers();
        LedgerCursor cursor = task.getCursor();
        boolean finished = true;
        Offset lastOffset = task.getPursueOffset();
        int count = 0;
        try {
            int runTimes = 0;
            ByteBuf entry;
            while ((entry = cursor.next()) != null) {
                runTimes++;
                ByteBuf payload = null;
                try {
                    Offset offset = MessageUtil.getOffset(entry);
                    if (!offset.after(lastOffset)) {
                        if (runTimes > pursueLimit) {
                            finished = false;
                            break;
                        }
                        continue;
                    }
                    lastOffset = offset;
                    int marker = MessageUtil.getMarker(entry);
                    if (!markers.contains(marker)) {
                        if (runTimes > pursueLimit) {
                            finished = false;
                            break;
                        }
                        continue;
                    }

                    subscription.setDispatchOffset(offset);
                    payload = createPayload(marker, offset, entry, channel.alloc());
                    count++;
                    if (channel.isWritable()) {
                        channel.writeAndFlush(payload.retainedSlice(), channel.voidPromise());
                    } else {
                        task.setPursueOffset(lastOffset);
                        compute(count);
                        channel.writeAndFlush(payload.retainedSlice(), delayPursue(task));
                        return;
                    }
                } catch (Throwable t) {
                    if (logger.isErrorEnabled()) {
                        logger.error("Pursue failed, task[{}] lastOffset[{}]", task, lastOffset, t);
                    }
                } finally {
                    ByteBufUtil.release(entry);
                    ByteBufUtil.release(payload);
                }

                if (runTimes > pursueLimit) {
                    finished = false;
                    break;
                }
            }
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("Pursue execute failed, task[{}] lastOffset[{}]", task, lastOffset, t);
            }
        }

        task.setPursueOffset(lastOffset);
        compute(count);
        Offset alignOffset = handler.getFollowOffset();
        if (finished || (alignOffset != null && !lastOffset.before(alignOffset))) {
            submitAlign(task);
        } else {
            submitPursue(task);
        }
    }

    private void submitFollow(PursueTask<DefaultSynchronization> task) {
        DefaultSynchronization subscription = task.getSubscription();
        try {
            subscription.getHandler().getDispatchExecutor().execute(() -> {
                subscription.setFollowed(true);
            });
        } catch (Throwable t) {
            subscription.setFollowed(true);
            if (logger.isErrorEnabled()) {
                logger.error("Submit failed, task[{}]", task);
            }
        }
    }

    private void submitAlign(PursueTask<DefaultSynchronization> task) {
        DefaultSynchronization subscription = task.getSubscription();
        try {
            subscription.getHandler().getDispatchExecutor().execute(() -> {
                doAlign(task);
            });
        } catch (Throwable t) {
            subscription.setFollowed(true);
            if (logger.isErrorEnabled()) {
                logger.error("Submit failed, task[{}]", task);
            }
        }
    }

    private void doAlign(PursueTask<DefaultSynchronization> task) {
        DefaultSynchronization subscription = task.getSubscription();
        Channel channel = subscription.getChannel();
        AbstractHandler<DefaultSynchronization, DefaultHandler> handler = subscription.getHandler();

        if (!channel.isActive() || subscription != handler.getSubscriptionChannels().get(channel)) {
            return;
        }

        Offset alignOffset = handler.getFollowOffset();
        Offset lastOffset = task.getPursueOffset();
        if (!lastOffset.before(alignOffset)) {
            subscription.setFollowed(true);
            return;
        }

        IntSet markers = subscription.getMarkers();
        LedgerCursor cursor = task.getCursor();
        boolean finished = true;
        int count = 0;
        try {
            int runTimes = 0;
            ByteBuf entry;
            while ((entry = cursor.next()) != null) {
                runTimes++;
                ByteBuf payload = null;
                try {
                    Offset offset = MessageUtil.getOffset(entry);
                    if (!offset.after(lastOffset)) {
                        if (runTimes > alignLimit) {
                            finished = false;
                            break;
                        }
                        continue;
                    }

                    if (offset.after(alignOffset)) {
                        subscription.setFollowed(true);
                        compute(count);
                        return;
                    }

                    lastOffset = offset;
                    int marker = MessageUtil.getMarker(entry);
                    if (!markers.contains(marker)) {
                        if (runTimes > pursueLimit) {
                            finished = false;
                            break;
                        }
                        continue;
                    }

                    subscription.setDispatchOffset(offset);
                    payload = createPayload(marker, offset, entry, channel.alloc());
                    count++;
                    if (channel.isWritable()) {
                        channel.writeAndFlush(payload.retainedSlice(), channel.voidPromise());
                    } else {
                        task.setPursueOffset(lastOffset);
                        compute(count);
                        channel.writeAndFlush(payload.retainedSlice(), delayPursue(task));
                        return;
                    }

                } catch (Throwable t) {
                    if (logger.isErrorEnabled()) {
                        logger.error("Switch to pursue, channel is full, task[{}]", task);
                    }
                } finally {
                    ByteBufUtil.release(entry);
                    ByteBufUtil.release(payload);
                }

                if (runTimes > alignLimit) {
                    finished = false;
                    break;
                }
            }
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("Pursue execute failed, task[{}] lastOffset[{}]", task, lastOffset, t);
            }
        }

        if (finished) {
            subscription.setFollowed(true);
            compute(count);
            return;
        }

        task.setPursueOffset(lastOffset);
        compute(count);
        submitPursue(task);
    }

    private ByteBuf createPayload(int marker, Offset offset, ByteBuf entry, ByteBufAllocator alloc) {
        ByteBuf buf = null;
        try {
            MessagePushSignal signal = MessagePushSignal.newBuilder()
                    .setEpoch(offset.getEpoch())
                    .setIndex(offset.getIndex())
                    .setMarker(marker)
                    .setLedger(ledger)
                    .build();
            int signalLength = ProtoBufUtil.protoLength(signal);
            int contentLength = entry.readableBytes() - 16;

            buf = alloc.ioBuffer(MessagePacket.HEADER_LENGTH + signalLength);
            buf.writeByte(MessagePacket.MAGIC_NUMBER);
            buf.writeMedium(MessagePacket.HEADER_LENGTH + signalLength + contentLength);
            buf.writeInt(Command.Client.PUSH_MESSAGE);
            buf.writeLong(0L);

            ProtoBufUtil.writeProto(buf, signal);
            buf = Unpooled.wrappedUnmodifiableBuffer(buf, entry.retainedSlice(entry.readerIndex() + 16, contentLength));

            return buf;
        } catch (Throwable t) {
            ByteBufUtil.release(buf);
            throw new DefaultDispatchException(
                    String.format("Build payload error, ledger[%d] topic[%s] offset[%s] length[%d]", ledger, t, offset,
                            entry.readableBytes()));
        }
    }

    public void dispatch() {
        if (dispatchHandlers.isEmpty()) {
            return;
        }
        for (DefaultHandler handler : dispatchHandlers) {
            if (handler.getFollowCursor() != null) {
                touchDispatch(handler);
            }
        }
    }

    private void detachMarker(Int2ObjectMap<Set<DefaultSynchronization>> markerSubscriptionMap, int marker,
                              DefaultSynchronization subscription) {
        Set<DefaultSynchronization> subscriptions = markerSubscriptionMap.get(marker);
        if (subscriptions != null) {
            subscriptions.remove(subscription);
            if (subscriptions.isEmpty()) {
                markerSubscriptionMap.remove(marker);
            }
        }
    }

    private void attachMarker(Int2ObjectMap<Set<DefaultSynchronization>> markerSubscriptionMap, int marker,
                              DefaultSynchronization subscription) {
        markerSubscriptionMap.computeIfAbsent(marker, k -> new ObjectArraySet<>()).add(subscription);
    }

    private void checkActive() {
        if (!isActive()) {
            throw new DefaultDispatchException("Dispatch handler is inactive");
        }
    }

    public boolean isActive() {
        return state.get();
    }

    public Future<Map<Channel, IntSet>> close(Promise<Map<Channel, IntSet>> promise) {
        Promise<Map<Channel, IntSet>> result = promise != null ? promise : ImmediateEventExecutor.INSTANCE.newPromise();
        if (state.compareAndSet(true, false)) {
            Map<Channel, IntSet> channelMarkers = new ConcurrentHashMap<>();
            AtomicInteger count = new AtomicInteger(executors.length);
            for (EventExecutor executor : executors) {
                try {
                    executor.submit(() -> {
                        for (Channel channel : channelHandlers.keySet()) {
                            if (channelExecutor(channel).inEventLoop()) {
                                AbstractHandler<DefaultSynchronization, DefaultHandler> handler =
                                        channelHandlers.get(channel);
                                ConcurrentMap<Channel, DefaultSynchronization> channelSubscriptionMap =
                                        handler == null ? null : handler.getSubscriptionChannels();
                                DefaultSynchronization
                                        subscription =
                                        channelSubscriptionMap == null ? null : channelSubscriptionMap.get(channel);
                                if (subscription != null) {
                                    channelMarkers.put(channel, subscription.getMarkers());
                                }
                                doClean(channel, ImmediateEventExecutor.INSTANCE.newPromise());
                            }
                        }

                        if (count.decrementAndGet() == 0) {
                            result.trySuccess(channelMarkers);
                        }
                    });
                } catch (Throwable t) {
                    result.tryFailure(t);
                }
            }

        } else {
            result.trySuccess(null);
        }
        return result;
    }

    private DefaultHandler allocateHandler(Channel channel) {
        DefaultHandler result = channelHandlers.get(channel);
        if (result != null) {
            return result;
        }
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int middleLimit = loadLimit >> 1;
        synchronized (weakHandlers) {
            if (weakHandlers.isEmpty()) {
                return DefaultHandler.INSTANCE.newHandler(weakHandlers, executors);
            }

            Map<DefaultHandler, Integer> selectHandlers = new HashMap<>();
            int randomBound = 0;
            for (DefaultHandler handler : weakHandlers.keySet()) {
                int channelCount = handler.getSubscriptionChannels().size();
                if (channelCount >= loadLimit) {
                    continue;
                }

                if (channelCount >= middleLimit) {
                    randomBound += loadLimit - channelCount;
                    selectHandlers.put(handler, channelCount);
                } else if (result == null || result.getSubscriptionChannels().size() < channelCount) {
                    result = handler;
                }
            }

            if (selectHandlers.isEmpty() || randomBound == 0) {
                return result != null ? result : DefaultHandler.INSTANCE.newHandler(weakHandlers, executors);
            }

            int index = random.nextInt(randomBound);
            int count = 0;
            for (Map.Entry<DefaultHandler, Integer> entry : selectHandlers.entrySet()) {
                count += loadLimit - entry.getValue();
                if (index < count) {
                    return entry.getKey();
                }
            }
            return result != null ? result : DefaultHandler.INSTANCE.newHandler(weakHandlers, executors);
        }
    }
}
