package org.ostara.dispatch;

import static org.ostara.remote.processor.ProcessCommand.Client.HANDLE_MESSAGE;
import static org.ostara.remote.util.ProtoBufUtils.protoLength;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.concurrent.ThreadSafe;
import org.ostara.internal.config.ServerConfig;
import org.ostara.ledger.Cursor;
import org.ostara.ledger.Offset;
import org.ostara.ledger.Storage;
import org.ostara.remote.Type;
import org.ostara.remote.codec.MessagePacket;
import org.ostara.remote.proto.notify.MessageSyncSignal;
import org.ostara.remote.util.ByteBufUtils;
import org.ostara.remote.util.ProtoBufUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class EntryChunkDispatchProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(EntryChunkDispatchProcessor.class);

    private final int ledger;
    private final String topic;
    private final Storage storage;
    private final int followLimit;
    private final int pursueLimit;
    private final int pursueTimeOutMs;
    private final int alignLimit;
    private final int loadLimit;
    private final int bytesLimit;

    private final EventExecutor[] executors;
    private final List<Handler> dispatchHandlers = new CopyOnWriteArrayList<>();
    private final ConcurrentHashMap<Channel, Handler> channelHandlerMap = new ConcurrentHashMap<>();
    private final WeakHashMap<Handler, Integer> allocateHandlers = new WeakHashMap<>();
    private final AtomicBoolean state = new AtomicBoolean(true);

    public EntryChunkDispatchProcessor(ServerConfig config, int ledger, String topic, Storage storage,
                                       EventExecutor[] executors) {
        this.ledger = ledger;
        this.topic = topic;
        this.storage = storage;
        this.followLimit = config.getChunkFollowLimit();
        this.pursueLimit = config.getChunkPursueLimit();
        this.pursueTimeOutMs = config.getChunkPursueTimeoutMs();
        this.loadLimit = config.getChunkLoadLimit();
        this.bytesLimit = config.getChunkBytesLimit();
        this.alignLimit = config.getChunkAlignLimit();
        this.executors = executors;
    }

    public int channelSize() {
        return channelHandlerMap.size();
    }

    private EventExecutor channelEventExecutor(Channel c) {
        return executors[(c.hashCode() & 0x7fffffff) % executors.length];
    }

    private Handler allocateHandler(Channel channel) {
        Handler handler = channelHandlerMap.get(channel);
        if (handler != null) {
            return handler;
        }

        ThreadLocalRandom random = ThreadLocalRandom.current();
        int middleLimit = loadLimit / 2;
        synchronized (allocateHandlers) {
            if (allocateHandlers.isEmpty()) {
                return createHandler();
            }

            Map<Handler, Integer> selectHandlers = new HashMap<>();
            int randomBound = 0;
            for (Handler h : allocateHandlers.keySet()) {
                int channelSize = h.channelSynchronizationMap.size();
                if (channelSize >= loadLimit) {
                    continue;
                }

                if (channelSize >= middleLimit) {
                    randomBound += loadLimit - channelSize;
                    selectHandlers.put(h, channelSize);
                } else if (handler == null || handler.channelSynchronizationMap.size() < channelSize) {
                    handler = h;
                }
            }

            if (selectHandlers.isEmpty() || randomBound == 0) {
                return handler != null ? handler : createHandler();
            }

            int index = random.nextInt(randomBound);
            int count = 0;
            for (Map.Entry<Handler, Integer> entry : selectHandlers.entrySet()) {
                count += loadLimit - entry.getValue();
                if (index < count) {
                    return entry.getKey();
                }
            }
        }
        return handler != null ? handler : createHandler();
    }

    private Handler createHandler() {
        synchronized (allocateHandlers) {
            int[] countArray = new int[executors.length];
            allocateHandlers.values().forEach(i -> countArray[i]++);

            int index = 0;
            if (countArray[index] > 0) {
                for (int i = 0; i < countArray.length; i++) {
                    int v = countArray[i];
                    if (v == 0) {
                        index = i;
                        break;
                    }

                    if (v < countArray[index]) {
                        index = i;
                    }
                }
            }

            Handler h = new Handler(executors[index]);
            allocateHandlers.put(h, index);
            return h;
        }
    }

    public void attach(Channel channel, Offset offset, Promise<Void> promise) {
        try {
            EventExecutor executor = channelEventExecutor(channel);
            if (executor.inEventLoop()) {
                doAttach(channel, offset, promise);
            } else {
                executor.execute(() -> doAttach(channel, offset, promise));
            }
        } catch (Throwable t) {
            promise.tryFailure(t);
        }
    }

    private void doAttach(Channel channel, Offset offset, Promise<Void> promise) {
        try {
            checkActive();

            Handler handler = allocateHandler(channel);
            ConcurrentHashMap<Channel, Synchronization> channelSynchronizationMap = handler.channelSynchronizationMap;
            Synchronization oldSynchronization = channelSynchronizationMap.get(channel);
            Synchronization newSynchronization = new Synchronization(channel, handler);

            handler.executor.execute(() -> {
                List<Synchronization> synchronizationList = handler.synchronizationList;
                if (oldSynchronization != null) {
                    synchronizationList.remove(oldSynchronization);
                }

                synchronizationList.add(newSynchronization);

                if (handler.cursor == null) {
                    handler.followOffset = storage.currentOffset();
                    handler.cursor = storage.locateCursor(handler.followOffset);
                    dispatchHandlers.add(handler);
                    touchDispatch(handler);
                }

                Offset dispatchOffset;
                if (offset != null) {
                    Offset earlyOffset = storage.headOffset();
                    if (earlyOffset.after(offset)) {
                        dispatchOffset = earlyOffset;
                    } else {
                        dispatchOffset = offset;
                    }

                    newSynchronization.dispatchOffset = dispatchOffset;
                    if (dispatchOffset.before(handler.followOffset)) {
                        PursueTask pursueTask = new PursueTask(newSynchronization, storage.locateCursor(dispatchOffset),
                                dispatchOffset);

                        submitPursueTask(pursueTask);
                    } else {
                        newSynchronization.followed = true;
                    }
                }
            });

            channelSynchronizationMap.put(channel, newSynchronization);
            channelHandlerMap.putIfAbsent(channel, handler);
            promise.trySuccess(null);

        } catch (Throwable t) {
            promise.tryFailure(t);
        }
    }

    private void checkActive() {
        if (state.get()) {
            throw new IllegalArgumentException("Chunk dispatch processor is inactive");
        }
    }

    private void touchDispatch(Handler handler) {
        if (handler.triggered.compareAndSet(false, true)) {
            try {
                handler.executor.execute(() -> doDispatch(handler));
            } catch (Throwable t) {
                LOGGER.error("Chunk dispatch submit failed, {}", handler, t);
            }
        }
    }

    private void submitPursueTask(PursueTask task) {
        try {
            channelEventExecutor(task.synchronization.channel).execute(() -> doPursueTask(task));
        } catch (Throwable t) {
            LOGGER.error(t.getMessage(), t);
            submitFollow(task);
        }
    }

    public void dispatch() {
        if (dispatchHandlers.isEmpty()) {
            return;
        }

        for (Handler h : dispatchHandlers) {
            if (h.cursor != null) {
                touchDispatch(h);
            }
        }
    }

    private void doDispatch(Handler handler) {
        Cursor cursor = handler.cursor;
        if (cursor == null) {
            handler.triggered.set(false);
            return;
        }

        List<Synchronization> synchronizationList = handler.synchronizationList;
        Offset lastOffset = handler.followOffset;

        try {
            int runTimes = 0;

            ChunkRecord record;
            while ((record = cursor.nextChunk(bytesLimit)) != null) {
                runTimes++;
                ByteBuf payload = null;
                try {
                    Offset endOffset = record.getEndOffset();
                    if (!endOffset.after(lastOffset)) {
                        if (runTimes > followLimit) {
                            break;
                        }
                        continue;
                    }

                    Offset startOffset = record.getStartOffset();
                    lastOffset = endOffset;
                    for (Synchronization synchronization : synchronizationList) {
                        if (synchronization.followed) {
                            continue;
                        }

                        Channel channel = synchronization.channel;
                        if (!channel.isActive()) {
                            continue;
                        }

                        if (!endOffset.after(synchronization.dispatchOffset)) {
                            continue;
                        }

                        synchronization.dispatchOffset = endOffset;

                        if (payload == null) {
                            payload = buildPayload(startOffset, endOffset, record, channel.alloc());
                        }

                        if (channel.isWritable()) {
                            channel.writeAndFlush(payload.retainedDuplicate(), channel.voidPromise());
                        } else {
                            synchronization.followed = false;
                            PursueTask pursueTask = new PursueTask(synchronization, cursor.clone(), endOffset);
                            channel.writeAndFlush(payload.retainedDuplicate(), delayPursue(pursueTask));
                        }
                    }
                } catch (Throwable ignored) {

                } finally {
                    record.data().release();
                    ByteBufUtils.release(payload);
                }

                if (runTimes > followLimit || record.count() <= 1) {
                    break;
                }
            }
        } catch (Throwable ignored) {

        } finally {
            handler.triggered.set(false);
        }

        handler.followOffset = lastOffset;
        if (cursor.hashNext()) {
            touchDispatch(handler);
        }
    }

    private ByteBuf buildPayload(Offset startOffset, Offset endOffset, ChunkRecord record, ByteBufAllocator alloc) {
        ByteBuf buf = null;
        try {
            MessageSyncSignal signal = MessageSyncSignal.newBuilder()
                    .setLedger(ledger)
                    .setCount(record.count())
                    .build();

            int signalLength = protoLength(signal);
            ByteBuf data = record.data();
            int bytes = data.readableBytes();

            buf = alloc.ioBuffer(MessagePacket.HEADER_LENGTH + signalLength);

            buf.writeByte(MessagePacket.MAGIC_NUMBER);
            buf.writeMedium(MessagePacket.HEADER_LENGTH + signalLength + bytes);
            buf.writeShort(-1);
            buf.writeByte(HANDLE_MESSAGE);
            buf.writeByte(Type.PUSH.sequence());
            buf.writeInt(0);

            ProtoBufUtils.writeProto(buf, signal);

            buf = Unpooled.wrappedUnmodifiableBuffer(buf, data.retainedSlice());

            return buf;
        } catch (Throwable t) {
            ByteBufUtils.release(buf);
            throw new RuntimeException(t);
        }
    }

    private ChannelPromise delayPursue(PursueTask task) {
        ChannelPromise promise = task.synchronization.channel.newPromise();
        promise.addListener((ChannelFutureListener) f -> {
            if (f.channel().isActive()) {
                submitPursue(task);
            }
        });

        return promise;
    }

    private void submitPursue(PursueTask task) {
        try {
            channelEventExecutor(task.synchronization.channel).execute(() -> doPursueTask(task));
        } catch (Throwable t) {
            submitFollow(task);
        }
    }

    private void doPursueTask(PursueTask task) {

    }

    private void submitFollow(PursueTask task) {
        try {
            task.synchronization.handler.executor.execute(() -> task.synchronization.followed = true);
        } catch (Throwable t) {
            task.synchronization.followed = true;
        }
    }

    public Future<Set<Channel>> close(Promise<Set<Channel>> promise) {
        Promise<Set<Channel>> p = promise != null ? promise : ImmediateEventExecutor.INSTANCE.newPromise();
        if (state.compareAndSet(true, false)) {
            Set<Channel> channels = new ConcurrentSkipListSet<>();
            AtomicInteger count = new AtomicInteger(executors.length);
            for (EventExecutor executor : executors) {
                try {
                    executor.submit(() -> {
                        for (Channel channel : channelHandlerMap.keySet()) {
                            if (channelEventExecutor(channel).inEventLoop()) {
                                doClear(channel, ImmediateEventExecutor.INSTANCE.newPromise());
                            }
                        }

                        if (count.decrementAndGet() == 0) {
                            p.trySuccess(channels);
                        }
                    });
                } catch (Throwable t) {
                    p.tryFailure(t);
                }
            }
        } else {
            p.trySuccess(null);
        }

        return p;
    }

    public void clear(Channel channel, Promise<Void> promise) {
        try {
            EventExecutor executor = channelEventExecutor(channel);
            if (executor.inEventLoop()) {
                doClear(channel, promise);
            } else {
                executor.execute(() -> doClear(channel, promise));
            }
        } catch (Throwable t) {
            promise.tryFailure(t);
        }
    }

    public void clearAll() {
        for (Channel channel : channelHandlerMap.keySet()) {
            clear(channel, ImmediateEventExecutor.INSTANCE.newPromise());
        }
    }

    private void doClear(Channel channel, Promise<Void> promise) {
        try {
            checkActive();

            Handler handler = channelHandlerMap.get(channel);
            if (handler == null) {
                promise.trySuccess(null);
                return;
            }

            ConcurrentHashMap<Channel, Synchronization> channelSynchronizationMap = handler.channelSynchronizationMap;
            Synchronization synchronization = channelSynchronizationMap.get(channel);
            if (synchronization == null) {
                promise.trySuccess(null);
                return;
            }

            handler.executor.execute(() -> {
                List<Synchronization> synchronizationList = handler.synchronizationList;
                synchronizationList.remove(synchronization);

                if (synchronizationList.isEmpty()) {
                    dispatchHandlers.remove(handler);
                    handler.cursor = null;
                    handler.followOffset = null;
                }
            });

            channelSynchronizationMap.remove(channel);
            channelHandlerMap.remove(channel);
            promise.trySuccess(null);
        } catch (Throwable t) {
            promise.tryFailure(t);
        }
    }

    private static class Handler {
        private final String id = UUID.randomUUID().toString();

        private final ConcurrentHashMap<Channel, Synchronization> channelSynchronizationMap = new ConcurrentHashMap<>();
        private final List<Synchronization> synchronizationList = new ArrayList<>();
        private final AtomicBoolean triggered = new AtomicBoolean(false);

        private volatile Offset followOffset;
        private final EventExecutor executor;
        private volatile Cursor cursor;

        public Handler(EventExecutor executor) {
            this.executor = executor;
        }

        @Override
        public String toString() {
            return "Handler{" +
                    "id='" + id + '\'' +
                    ",Channels=" + channelSynchronizationMap.size() +
                    ", followOffset=" + followOffset +
                    '}';
        }
    }

    private static class Synchronization {
        private final Channel channel;
        private final Handler handler;
        private Offset dispatchOffset;
        private boolean followed = false;

        public Synchronization(Channel channel, Handler handler) {
            this.channel = channel;
            this.handler = handler;
        }

        @Override
        public String toString() {
            return "Synchronization{" +
                    "channel=" + channel +
                    ", handler=" + handler +
                    ", dispatchOffset=" + dispatchOffset +
                    '}';
        }
    }

    private static class PursueTask {

        private final Synchronization synchronization;
        private final Cursor cursor;
        private final long pursueTime = System.currentTimeMillis();
        private final Offset pursueOffset;

        public PursueTask(Synchronization synchronization, Cursor cursor, Offset pursueOffset) {
            this.synchronization = synchronization;
            this.cursor = cursor;
            this.pursueOffset = pursueOffset;
        }

        @Override
        public String toString() {
            return "PursueTask{" +
                    "synchronization=" + synchronization +
                    ", cursor=" + cursor +
                    ", pursueTime=" + pursueTime +
                    ", pursueOffset=" + pursueOffset +
                    '}';
        }
    }
}
