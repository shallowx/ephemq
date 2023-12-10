package org.ostara.ledger;

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
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.ObjectArraySet;
import org.ostara.common.Offset;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.common.util.MessageUtils;
import org.ostara.core.CoreConfig;
import org.ostara.remote.codec.MessagePacket;
import org.ostara.remote.processor.ProcessCommand;
import org.ostara.remote.proto.client.MessagePushSignal;
import org.ostara.remote.util.ByteBufUtils;
import org.ostara.remote.util.ProtoBufUtils;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;

public class RecordEntryDispatcher {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(RecordEntryDispatcher.class);

    private final int ledger;
    private final String topic;
    private final LedgerStorage storage;
    private final int followLimit;
    private final int pursueLimit;
    private final int alignLimit;
    private final long pursueTimeoutMs;
    private final int loadLimit;
    private final IntConsumer counter;
    private final EventExecutor[] executors;
    private final List<Handler> dispatchHandlers = new CopyOnWriteArrayList<>();
    private final WeakHashMap<Handler, Integer> allocateHandlers = new WeakHashMap<>();
    private final ConcurrentMap<Channel, Handler> channelHandlerMap = new ConcurrentHashMap<>();
    private final AtomicBoolean state = new AtomicBoolean(true);

    RecordEntryDispatcher(int ledger, String topic, LedgerStorage storage, CoreConfig config, EventExecutorGroup group,
                          IntConsumer dispatchCounter) {
        this.ledger = ledger;
        this.topic = topic;
        this.storage = storage;
        this.followLimit = config.getDispatchEntryFollowLimit();
        this.pursueLimit = config.getDispatchEntryPursueLimit();
        this.alignLimit = config.getDispatchEntryAlignLimit();
        this.pursueTimeoutMs = config.getDispatchEntryPursueTimeoutMs();
        this.loadLimit = config.getDispatchEntryLoadLimit();
        this.counter = dispatchCounter;

        List<EventExecutor> eventExecutors = new ArrayList<>();
        group.forEach(eventExecutors::add);
        Collections.shuffle(eventExecutors);
        this.executors = eventExecutors.toArray(new EventExecutor[0]);
    }

    public int channelCount() {
        return channelHandlerMap.size();
    }

    private EventExecutor channelExecutor(Channel channel) {
        return executors[(channel.hashCode() & 0x7fffffff) % executors.length];
    }

    private Handler allocateHandler(Channel channel) {
        Handler result = channelHandlerMap.get(channel);
        if (result != null) {
            return result;
        }
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int middleLimit = loadLimit >> 1;
        synchronized (allocateHandlers) {
            if (allocateHandlers.isEmpty()) {
                return createHandler();
            }

            Map<Handler, Integer> selectHandlers = new HashMap<>();
            int randomBound = 0;
            for (Handler handler : allocateHandlers.keySet()) {
                int channelCount = handler.channelSubscriptionMap.size();
                if (channelCount >= loadLimit) {
                    continue;
                }

                if (channelCount >= middleLimit) {
                    randomBound += loadLimit - channelCount;
                    selectHandlers.put(handler, channelCount);
                } else if (result == null || result.channelSubscriptionMap.size() < channelCount) {
                    result = handler;
                }
            }

            if (selectHandlers.isEmpty() || randomBound == 0) {
                return result != null ? result : createHandler();
            }

            int index = random.nextInt(randomBound);
            int count = 0;
            for (Map.Entry<Handler, Integer> entry : selectHandlers.entrySet()) {
                count += loadLimit - entry.getValue();
                if (index < count) {
                    return entry.getKey();
                }
            }
            return result != null ? result : createHandler();
        }
    }

    private Handler createHandler() {
        synchronized (allocateHandlers) {
            int[] countArray = new int[executors.length];
            allocateHandlers.values().forEach(i -> countArray[i]++);
            int index = 0;
            if (countArray[index] > 0) {
                for (int i = 1; i < countArray.length; i++) {
                    int v = countArray[i];
                    if (v == 0) {
                        index = i;
                        break;
                    }

                    if (v < countArray[i]) {
                        index = i;
                    }
                }
            }

            Handler result = new Handler(executors[index]);
            allocateHandlers.put(result, index);
            return result;
        }
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

            Handler handler = allocateHandler(channel);
            ConcurrentMap<Channel, Subscription> channelSubscriptionMap = handler.channelSubscriptionMap;
            Subscription oldSubscription = channelSubscriptionMap.get(channel);
            Subscription newSubscription = new Subscription(channel, new IntOpenHashSet(wholeMarkers), handler);
            handler.dispatchExecutor.execute(() -> {
                Int2ObjectMap<Set<Subscription>> markerSubscriptionMap = handler.markerSubscriptionMap;
                if (oldSubscription != null) {
                    oldSubscription.markers.forEach((int marker) -> detachMarker(markerSubscriptionMap, marker, newSubscription));
                }

                wholeMarkers.forEach((int marker) -> attachMarker(markerSubscriptionMap, marker, newSubscription));
                if (handler.followCursor == null) {
                    handler.followOffset = storage.currentOffset();
                    handler.followCursor = storage.cursor(handler.followOffset);
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

                newSubscription.dispatchOffset = dispatchOffset;
                if (dispatchOffset.before(handler.followOffset)) {
                    PursueTask task = new PursueTask(newSubscription, storage.cursor(dispatchOffset), dispatchOffset);
                    submitPursue(task);
                } else {
                    newSubscription.followed = true;
                }
            });

            channelSubscriptionMap.put(channel, newSubscription);
            channelHandlerMap.putIfAbsent(channel, handler);
            promise.trySuccess(newSubscription.markers.size());
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
            Handler handler = channelHandlerMap.get(channel);
            ConcurrentMap<Channel, Subscription> channelSubscriptionMap = handler == null ? null : handler.channelSubscriptionMap;
            Subscription subscription = channelSubscriptionMap == null ? null : channelSubscriptionMap.get(channel);
            if (subscription == null) {
                promise.tryFailure(new IllegalArgumentException("Alter is invalid"));
                return;
            }

            handler.dispatchExecutor.execute(() -> {
                Int2ObjectMap<Set<Subscription>> markerSubscriptionMap = handler.markerSubscriptionMap;
                deleteMarkers.forEach((int marker) -> detachMarker(markerSubscriptionMap, marker, subscription));
                appendMarkers.forEach((int marker) -> attachMarker(markerSubscriptionMap, marker, subscription));

                if (markerSubscriptionMap.isEmpty()) {
                    dispatchHandlers.remove(handler);
                    handler.followCursor = null;
                    handler.followOffset = null;
                }
            });

            IntSet markers = subscription.markers;
            markers.removeAll(deleteMarkers);
            markers.addAll(appendMarkers);
            if (markers.isEmpty()) {
                channelSubscriptionMap.remove(channel);
                channelHandlerMap.remove(channel);
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

            Handler handler = channelHandlerMap.get(channel);
            ConcurrentMap<Channel, Subscription> channelSubscriptionMap = handler == null ? null : handler.channelSubscriptionMap;
            Subscription subscription = channelSubscriptionMap == null ? null : channelSubscriptionMap.get(channel);
            if (subscription == null) {
                promise.trySuccess(false);
                return;
            }

            handler.dispatchExecutor.execute(() -> {
                Int2ObjectMap<Set<Subscription>> markerSubscriptionMap = handler.markerSubscriptionMap;
                subscription.markers.forEach((int marker) -> detachMarker(markerSubscriptionMap, marker, subscription));
                if (markerSubscriptionMap.isEmpty()) {
                    dispatchHandlers.remove(handler);
                    handler.followOffset = null;
                    handler.followCursor = null;
                }
            });

            channelSubscriptionMap.remove(channel);
            channelHandlerMap.remove(channel);
            promise.trySuccess(true);
        } catch (Throwable t) {
            promise.tryFailure(t);
        }
    }

    private void touchDispatch(Handler handler) {
        if (handler.triggered.compareAndSet(false, true)) {
            try {
                handler.dispatchExecutor.execute(() -> doDispatch(handler));
            } catch (Throwable t) {
                logger.error("Dispatch submit failed: {}", handler, t);
            }
        }
    }

    private void doDispatch(Handler handler) {
        LedgerCursor cursor = handler.followCursor;
        if (cursor == null) {
            handler.triggered.set(false);
            return;
        }

        Int2ObjectMap<Set<Subscription>> markerSubscriptionMap = handler.markerSubscriptionMap;
        Offset lastOffset = handler.followOffset;
        int count = 0;
        try {
            int runTimes = 0;
            ByteBuf entry;
            while ((entry = cursor.next()) != null) {
                runTimes++;
                ByteBuf payload = null;
                try {
                    Offset offset = MessageUtils.getOffset(entry);
                    if (!offset.after(lastOffset)) {
                        if (runTimes > followLimit) {
                            break;
                        }
                        continue;
                    }

                    lastOffset = offset;
                    int marker = MessageUtils.getMarker(entry);
                    Set<Subscription> subscriptions = markerSubscriptionMap.get(marker);
                    if (subscriptions == null) {
                        if (runTimes > followLimit) {
                            break;
                        }
                        continue;
                    }

                    for (Subscription subscription : subscriptions) {
                        if (!subscription.followed) {
                            continue;
                        }
                        Channel channel = subscription.channel;
                        if (!channel.isActive()) {
                            continue;
                        }

                        if (!offset.after(subscription.dispatchOffset)) {
                            continue;
                        }

                        subscription.dispatchOffset = offset;
                        if (payload == null) {
                            payload = buildPayload(marker, offset, entry, channel.alloc());
                        }

                        count++;
                        if (channel.isWritable()) {
                            channel.writeAndFlush(payload.retainedSlice(), channel.voidPromise());
                        } else {
                            subscription.followed = false;
                            PursueTask task = new PursueTask(subscription, cursor.copy(), offset);
                            channel.writeAndFlush(payload.retainedSlice(), delayPursue(task));
                        }
                    }
                } catch (Throwable t) {
                    logger.error("Dispatch failed: {} lastOffset={}", handler, lastOffset, t);
                } finally {
                    ByteBufUtils.release(entry);
                    ByteBufUtils.release(payload);
                }
                if (runTimes > followLimit) {
                    break;
                }
            }
        } catch (Throwable t) {
            logger.error("Dispatch execute failed: {} lastOffset={}", handler, lastOffset, t);
        } finally {
            handler.triggered.set(false);
        }

        handler.followOffset = lastOffset;
        count(count);
        if (cursor.hashNext()) {
            touchDispatch(handler);
        }
    }

    private void count(int count) {
        if (count > 0) {
            try {
                counter.accept(count);
            } catch (Throwable t) {
                logger.error("Count failed, ledger={} topic={}", ledger, topic, t);
            }
        }
    }

    private ChannelPromise delayPursue(PursueTask task) {
        ChannelPromise promise = task.subscription.channel.newPromise();
        promise.addListener((ChannelFutureListener) f -> {
            if (f.channel().isActive()) {
                submitPursue(task);
            }
        });
        return promise;
    }

    private void submitPursue(PursueTask task) {
        try {
            channelExecutor(task.subscription.channel).execute(() -> {
                doPursue(task);
            });
        } catch (Throwable t) {
            task.subscription.followed = true;
            logger.error("Submit failed: {}", task);
        }
    }

    private void doPursue(PursueTask task) {
        Subscription subscription = task.subscription;
        Channel channel = subscription.channel;
        Handler handler = subscription.handler;

        if (!channel.isActive() || subscription != handler.channelSubscriptionMap.get(channel)) {
            return;
        }

        if (System.currentTimeMillis() - task.pursueTime > pursueTimeoutMs) {
            logger.warn("Giving up pursue:{}", task);
            submitFollow(task);
            return;
        }

        IntSet markers = subscription.markers;
        LedgerCursor cursor = task.cursor;
        boolean finished = true;
        Offset lastOffset = task.pursueOffset;
        int count = 0;
        try {
            int runTimes = 0;
            ByteBuf entry;
            while ((entry = cursor.next()) != null) {
                runTimes++;
                ByteBuf payload = null;
                try {
                    Offset offset = MessageUtils.getOffset(entry);
                    if (!offset.after(lastOffset)) {
                        if (runTimes > pursueLimit) {
                            finished = false;
                            break;
                        }
                        continue;
                    }
                    lastOffset = offset;
                    int marker = MessageUtils.getMarker(entry);
                    if (!markers.contains(marker)) {
                        if (runTimes > pursueLimit) {
                            finished = false;
                            break;
                        }
                        continue;
                    }

                    subscription.dispatchOffset = offset;
                    payload = buildPayload(marker, offset, entry, channel.alloc());
                    count++;
                    if (channel.isWritable()) {
                        channel.writeAndFlush(payload.retainedSlice(), channel.voidPromise());
                    } else {
                        task.pursueOffset = lastOffset;
                        count(count);
                        channel.writeAndFlush(payload.retainedSlice(), delayPursue(task));
                        return;
                    }
                } catch (Throwable t) {
                    logger.error("Pursue failed:{} lastOffset={}", task, lastOffset, t);
                } finally {
                    ByteBufUtils.release(entry);
                    ByteBufUtils.release(payload);
                }

                if (runTimes > pursueLimit) {
                    finished = false;
                    break;
                }
            }
        } catch (Throwable t) {
            logger.error("Pursue execute failed:{} lastOffset={}", task, lastOffset, t);
        }

        task.pursueOffset = lastOffset;
        count(count);
        Offset alignOffset = handler.followOffset;
        if (finished || (alignOffset != null && !lastOffset.before(alignOffset))) {
            submitAlign(task);
        } else {
            submitPursue(task);
        }
    }

    private void submitFollow(PursueTask task) {
        try {
            task.subscription.handler.dispatchExecutor.execute(() -> {
                task.subscription.followed = true;
            });
        } catch (Throwable t) {
            task.subscription.followed = true;
            logger.error("Submit failed: {}", task);
        }
    }

    private void submitAlign(PursueTask task) {
        try {
            task.subscription.handler.dispatchExecutor.execute(() -> {
                doAlign(task);
            });
        } catch (Throwable t) {
            task.subscription.followed = true;
            logger.error("Submit failed: {}", task);
        }
    }

    private void doAlign(PursueTask task) {
        Subscription subscription = task.subscription;
        Channel channel = subscription.channel;
        Handler handler = subscription.handler;

        if (!channel.isActive() || subscription != handler.channelSubscriptionMap.get(channel)) {
            return;
        }

        Offset alignOffset = handler.followOffset;
        Offset lastOffset = task.pursueOffset;
        if (!lastOffset.before(alignOffset)) {
            subscription.followed = true;
            return;
        }

        IntSet markers = subscription.markers;
        LedgerCursor cursor = task.cursor;
        boolean finished = true;
        int count = 0;
        try {
            int runTimes = 0;
            ByteBuf entry;
            while ((entry = cursor.next()) != null) {
                runTimes++;
                ByteBuf payload = null;
                try {
                    Offset offset = MessageUtils.getOffset(entry);
                    if (!offset.after(lastOffset)) {
                        if (runTimes > alignLimit) {
                            finished = false;
                            break;
                        }
                        continue;
                    }

                    if (offset.after(alignOffset)) {
                        subscription.followed = true;
                        count(count);
                        return;
                    }
                    lastOffset = offset;
                    int marker = MessageUtils.getMarker(entry);
                    if (!markers.contains(marker)) {
                        if (runTimes > pursueLimit) {
                            finished = false;
                            break;
                        }
                        continue;
                    }

                    subscription.dispatchOffset = offset;
                    payload = buildPayload(marker, offset, entry, channel.alloc());
                    count++;
                    if (channel.isWritable()) {
                        channel.writeAndFlush(payload.retainedSlice(), channel.voidPromise());
                    } else {
                        task.pursueOffset = lastOffset;
                        count(count);
                        channel.writeAndFlush(payload.retainedSlice(), delayPursue(task));
                        return;
                    }

                } catch (Throwable t) {
                    logger.error("Switch to pursue, channel is full:{}", task);
                } finally {
                    ByteBufUtils.release(entry);
                    ByteBufUtils.release(payload);
                }

                if (runTimes > alignLimit) {
                    finished = false;
                    break;
                }
            }
        } catch (Throwable t) {
            logger.error("Pursue execute failed:{} lastOffset={}", task, lastOffset, t);
        }

        if (finished) {
            subscription.followed = true;
            count(count);
            return;
        }

        task.pursueOffset = lastOffset;
        count(count);
        submitPursue(task);
    }


    private ByteBuf buildPayload(int marker, Offset offset, ByteBuf entry, ByteBufAllocator alloc) {
        ByteBuf buf = null;
        try {
            MessagePushSignal signal = MessagePushSignal.newBuilder()
                    .setEpoch(offset.getEpoch())
                    .setIndex(offset.getIndex())
                    .setMarker(marker)
                    .setLedger(ledger)
                    .build();
            int signalLength = ProtoBufUtils.protoLength(signal);
            int contentLength = entry.readableBytes() - 16;

            buf = alloc.ioBuffer(MessagePacket.HEADER_LENGTH + signalLength);
            buf.writeByte(MessagePacket.MAGIC_NUMBER);
            buf.writeMedium(MessagePacket.HEADER_LENGTH + signalLength + contentLength);
            buf.writeInt(ProcessCommand.Client.PUSH_MESSAGE);
            buf.writeInt(0);

            ProtoBufUtils.writeProto(buf, signal);
            buf = Unpooled.wrappedUnmodifiableBuffer(buf, entry.retainedSlice(entry.readerIndex() + 16, contentLength));

            return buf;
        } catch (Throwable t) {
            ByteBufUtils.release(buf);
            throw new RuntimeException(String.format("Build payload error, ledger=%d topic=%s offset=%s length=%d", ledger, t, offset, entry.readableBytes()));
        }
    }

    public void dispatch() {
        if (dispatchHandlers.isEmpty()) {
            return;
        }

        for (Handler handler : dispatchHandlers) {
            if (handler.followCursor != null) {
                touchDispatch(handler);
            }
        }
    }

    private void detachMarker(Int2ObjectMap<Set<Subscription>> markerSubscriptionMap, int marker, Subscription subscription) {
        Set<Subscription> subscriptions = markerSubscriptionMap.get(marker);
        if (subscriptions != null) {
            subscriptions.remove(subscription);
            if (subscriptions.isEmpty()) {
                markerSubscriptionMap.remove(marker);
            }
        }
    }

    private void attachMarker(Int2ObjectMap<Set<Subscription>> markerSubscriptionMap, int marker, Subscription subscription) {
        markerSubscriptionMap.computeIfAbsent(marker, k -> new ObjectArraySet<>()).add(subscription);
    }

    private void checkActive() {
        if (!isActive()) {
            throw new IllegalArgumentException("Dispatch handler is inactive");
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
                        for (Channel channel : channelHandlerMap.keySet()) {
                            if (channelExecutor(channel).inEventLoop()) {
                                Handler handler = channelHandlerMap.get(channel);
                                ConcurrentMap<Channel, Subscription> channelSubscriptionMap = handler == null ? null : handler.channelSubscriptionMap;
                                Subscription subscription = channelSubscriptionMap == null ? null : channelSubscriptionMap.get(channel);
                                if (subscription != null) {
                                    channelMarkers.put(channel, subscription.markers);
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

    private static class PursueTask {
        private final Subscription subscription;
        private final LedgerCursor cursor;
        private final long pursueTime = System.currentTimeMillis();
        private Offset pursueOffset;

        public PursueTask(Subscription subscription, LedgerCursor cursor, Offset pursueOffset) {
            this.subscription = subscription;
            this.cursor = cursor;
            this.pursueOffset = pursueOffset;
        }

        @Override
        public String toString() {
            return "PursueTask{" +
                    "subscription=" + subscription +
                    ", cursor=" + cursor +
                    ", pursueTime=" + pursueTime +
                    ", pursueOffset=" + pursueOffset +
                    '}';
        }
    }

    private static class Handler {
        private final String id = UUID.randomUUID().toString();
        private final ConcurrentMap<Channel, Subscription> channelSubscriptionMap = new ConcurrentHashMap<>();
        private final Int2ObjectMap<Set<Subscription>> markerSubscriptionMap = new Int2ObjectOpenHashMap<>();
        private final AtomicBoolean triggered = new AtomicBoolean(false);
        private final EventExecutor dispatchExecutor;
        private volatile Offset followOffset;
        private volatile LedgerCursor followCursor;

        private Handler(EventExecutor executor) {
            this.dispatchExecutor = executor;
        }

        @Override
        public String toString() {
            return "Handler{" +
                    "id='" + id + '\'' +
                    ", channelSubscriptionMap=" + channelSubscriptionMap +
                    ", markerSubscriptionMap=" + markerSubscriptionMap +
                    ", triggered=" + triggered +
                    ", dispatchExecutor=" + dispatchExecutor +
                    ", followOffset=" + followOffset +
                    ", followCursor=" + followCursor +
                    '}';
        }
    }

    private static class Subscription {
        private final Channel channel;
        private final IntSet markers;
        private final Handler handler;
        private Offset dispatchOffset;
        private boolean followed = false;

        public Subscription(Channel channel, IntSet markers, Handler handler) {
            this.channel = channel;
            this.markers = markers;
            this.handler = handler;
        }

        @Override
        public String toString() {
            return "Subscription{" +
                    "channel=" + channel +
                    ", markers=" + markers +
                    ", handler=" + handler +
                    ", dispatchOffset=" + dispatchOffset +
                    ", followed=" + followed +
                    '}';
        }
    }
}
