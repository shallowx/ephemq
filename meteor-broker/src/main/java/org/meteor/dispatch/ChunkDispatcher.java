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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.common.message.Offset;
import org.meteor.common.util.MessageUtil;
import org.meteor.config.ChunkDispatchConfig;
import org.meteor.exception.ChunkDispatchException;
import org.meteor.ledger.ChunkRecord;
import org.meteor.ledger.LedgerCursor;
import org.meteor.ledger.LedgerStorage;
import org.meteor.remote.codec.MessagePacket;
import org.meteor.remote.invoke.Command;
import org.meteor.remote.proto.client.SyncMessageSignal;
import org.meteor.remote.util.ByteBufUtil;
import org.meteor.remote.util.ProtoBufUtil;

public class ChunkDispatcher {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ChunkDispatcher.class);
    private final int ledger;
    private final String topic;
    private final LedgerStorage storage;
    private final int followLimit;
    private final int pursueLimit;
    private final int alignLimit;
    private final long pursueTimeoutMilliseconds;
    private final int loadLimit;
    private final int pursueBytesLimit;
    private final int bytesLimit;
    private final IntConsumer counter;
    private final EventExecutor[] executors;
    private final List<ChunkHandler> dispatchHandlers = new CopyOnWriteArrayList<>();
    private final WeakHashMap<ChunkHandler, Integer> weakHandlers = new WeakHashMap<>();
    private final ConcurrentMap<Channel, ChunkHandler> channelHandlers = new ConcurrentHashMap<>();
    private final AtomicBoolean state = new AtomicBoolean(true);

    public ChunkDispatcher(int ledger, String topic, LedgerStorage storage, ChunkDispatchConfig config,
                           EventExecutorGroup executorGroup, IntConsumer dispatchCounter) {
        this.ledger = ledger;
        this.topic = topic;
        this.storage = storage;
        this.followLimit = config.getChunkDispatchEntryFollowLimit();
        this.pursueLimit = config.getChunkDispatchEntryPursueLimit();
        this.alignLimit = config.getChunkDispatchEntryAlignLimit();
        this.pursueTimeoutMilliseconds = config.getChunkDispatchEntryPursueTimeoutMilliseconds();
        this.loadLimit = config.getChunkDispatchEntryLoadLimit();
        this.bytesLimit = config.getChunkDispatchEntryBytesLimit();
        this.pursueBytesLimit = bytesLimit + config.getChunkDispatchEntryPursueLimit();
        this.counter = dispatchCounter;

        List<EventExecutor> eventExecutors = new ArrayList<>();
        executorGroup.forEach(eventExecutors::add);
        Collections.shuffle(eventExecutors);
        this.executors = eventExecutors.toArray(new EventExecutor[0]);
    }

    public int channelCount() {
        return channelHandlers.size();
    }

    private EventExecutor channelExecutor(Channel channel) {
        return executors[(channel.hashCode() & 0x7fffffff) % executors.length];
    }

    public void cancelSubscribes() {
        for (Channel channel : channelHandlers.keySet()) {
            cancelSubscribe(channel, ImmediateEventExecutor.INSTANCE.newPromise());
        }
    }

    public void cancelSubscribe(Channel channel, Promise<Void> promise) {
        try {
            EventExecutor executor = channelExecutor(channel);
            if (executor.inEventLoop()) {
                doCancelSubscribe(channel, promise);
            } else {
                executor.execute(() -> doCancelSubscribe(channel, promise));
            }
        } catch (Exception e) {
            promise.tryFailure(e);
        }
    }

    private void doCancelSubscribe(Channel channel, Promise<Void> promise) {
        try {
            if (!state.get()) {
                throw new IllegalStateException("Chunk dispatcher is inactive");
            }
            ChunkHandler handler = channelHandlers.get(channel);
            if (handler == null) {
                promise.trySuccess(null);
                return;
            }
            ConcurrentMap<Channel, ChunkSynchronization> channelSynchronizationMap = handler.getSubscriptionChannels();
            ChunkSynchronization synchronization = channelSynchronizationMap.get(channel);
            if (synchronization == null) {
                promise.trySuccess(null);
                return;
            }

            handler.dispatchExecutor.execute(() -> {
                List<ChunkSynchronization> synchronizations = handler.getSynchronizations();
                synchronizations.remove(synchronization);
                if (synchronizations.isEmpty()) {
                    dispatchHandlers.remove(handler);
                    handler.followCursor = null;
                    handler.followOffset = null;
                }
            });

            channelSynchronizationMap.remove(channel);
            channelHandlers.remove(channel);
            promise.trySuccess(null);
        } catch (Exception e) {
            promise.tryFailure(e);
        }
    }

    public void dispatch() {
        if (dispatchHandlers.isEmpty()) {
            return;
        }
        for (ChunkHandler handler : dispatchHandlers) {
            if (handler.followCursor != null) {
                touchDispatch(handler);
            }
        }
    }

    private void touchDispatch(ChunkHandler handler) {
        if (handler.triggered.compareAndSet(false, true)) {
            try {
                handler.dispatchExecutor.execute(() -> doDispatch(handler));
            } catch (Exception e) {
                if (logger.isErrorEnabled()) {
                    logger.error("Chunk submit dispatch failed", e);
                }
            }
        }
    }

    private void doDispatch(ChunkHandler handler) {
        LedgerCursor cursor = handler.followCursor;
        if (cursor == null) {
            handler.triggered.set(false);
            return;
        }

        List<ChunkSynchronization> synchronizations = handler.getSynchronizations();
        Offset lastOffset = handler.followOffset;
        int count = 0;
        try {
            int runTimes = 0;
            ChunkRecord chunk;
            while ((chunk = cursor.nextChunk(bytesLimit)) != null) {
                runTimes++;
                ByteBuf payload = null;
                try {
                    Offset endOffset = chunk.getEndOffset();
                    if (!endOffset.after(lastOffset)) {
                        if (runTimes > followLimit) {
                            break;
                        }
                        continue;
                    }

                    Offset startOffset = chunk.getStartOffset();
                    if (!MessageUtil.isContinuous(lastOffset, startOffset) && logger.isDebugEnabled()) {
                        logger.debug("Chunk met discontinuous message, handler[{}], baseOffset[{}], nextOffset[{}], runtimes[{}]",
                                handler, lastOffset, startOffset, runTimes);
                    }

                    lastOffset = endOffset;
                    for (ChunkSynchronization synchronization : synchronizations) {
                        if (!synchronization.followed) {
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
                            payload = createPayload(startOffset, endOffset, chunk, channel.alloc());
                        }

                        count += chunk.count();
                        if (channel.isWritable()) {
                            channel.writeAndFlush(payload.retainedDuplicate(), channel.voidPromise());
                        } else {
                            synchronization.followed = true;
                            PursueTask<ChunkSynchronization> pursueTask =
                                    new PursueTask<>(synchronization, cursor.copy(), endOffset);
                            channel.writeAndFlush(payload.retainedDuplicate(), delayPursue(pursueTask));
                        }
                    }
                } catch (Exception e) {
                    if (logger.isErrorEnabled()) {
                        logger.error("Chunk dispatch failed, handler[{}] lastOffset[{}]", handler, lastOffset, e);
                    }
                } finally {
                    ByteBufUtil.release(chunk.data());
                    ByteBufUtil.release(payload);
                }

                if (runTimes > followLimit || chunk.count() <= 1) {
                    break;
                }
            }
        } catch (Exception e) {
            if (logger.isErrorEnabled()) {
                logger.error("Chunk dispatch failed, handler[{}], lastOffset[{}]", handler, lastOffset, e);
            }
        } finally {
            handler.triggered.set(false);
        }

        handler.followOffset = lastOffset;
        compute(count);
        if (cursor.hashNext()) {
            touchDispatch(handler);
        }
    }

    private ByteBuf createPayload(Offset startOffset, Offset endOffset, ChunkRecord chunk, ByteBufAllocator alloc) {
        ByteBuf buf = null;
        try {
            SyncMessageSignal signal = SyncMessageSignal.newBuilder()
                    .setLedger(ledger)
                    .setCount(chunk.count())
                    .build();
            int length = ProtoBufUtil.protoLength(signal);
            ByteBuf data = chunk.data();
            int contextLength = data.readableBytes();
            buf = alloc.ioBuffer(MessagePacket.HEADER_LENGTH + length);
            buf.writeByte(MessagePacket.MAGIC_NUMBER);
            buf.writeMedium(MessagePacket.HEADER_LENGTH + length + contextLength);
            buf.writeInt(Command.Client.SYNC_MESSAGE);
            buf.writeLong(0L);
            ProtoBufUtil.writeProto(buf, signal);
            buf = Unpooled.wrappedUnmodifiableBuffer(buf, data.retainedSlice());
            return buf;
        } catch (Exception e) {
            ByteBufUtil.release(buf);
            throw new ChunkDispatchException(String.format(
                    "Build payload error, ledger[%d] topic[%s] startOffset[%s] ednOffset[%s] length[%d]",
                    ledger, topic, startOffset, endOffset, chunk.data().readableBytes()
            ));
        }
    }

    private void compute(int count) {
        if (count > 0) {
            try {
                counter.accept(count);
            } catch (Exception e) {
                if (logger.isErrorEnabled()) {
                    logger.error("Chunk count failed, ledger[{}] topic[{}]", ledger, topic, e);
                }
            }
        }
    }

    private ChannelPromise delayPursue(PursueTask<ChunkSynchronization> pursueTask) {
        ChannelPromise promise = pursueTask.getSubscription().channel.newPromise();
        promise.addListener((ChannelFutureListener) f -> {
            if (f.channel().isActive()) {
                submitPursue(pursueTask);
            }
        });
        return promise;
    }

    private void submitPursue(PursueTask<ChunkSynchronization> pursueTask) {
        try {
            channelExecutor(pursueTask.getSubscription().channel).execute(() -> doPursue(pursueTask));
        } catch (Exception e) {
            submitFollow(pursueTask);
        }
    }

    private void doPursue(PursueTask<ChunkSynchronization> pursueTask) {
        ChunkSynchronization synchronization = pursueTask.getSubscription();
        Channel channel = synchronization.channel;
        ChunkHandler handler = synchronization.handler;
        if (!channel.isActive() || synchronization != handler.getSubscriptionChannels().get(channel)) {
            return;
        }

        if (System.currentTimeMillis() - pursueTask.getPursueTimeMillis() > pursueTimeoutMilliseconds) {
            submitFollow(pursueTask);
            return;
        }

        LedgerCursor cursor = pursueTask.getCursor();
        boolean finished = true;
        Offset lastOffset = pursueTask.getPursueOffset();
        int count = 0;
        try {
            int runtimes = 0;
            ChunkRecord chunk;
            while ((chunk = cursor.nextChunk(pursueBytesLimit)) != null) {
                runtimes++;
                ByteBuf payload = null;
                try {
                    Offset endOffset = chunk.getEndOffset();
                    if (!endOffset.after(lastOffset)) {
                        if (runtimes > pursueLimit) {
                            finished = false;
                            break;
                        }
                        continue;
                    }
                    Offset startOffset = chunk.getStartOffset();
                    if (!MessageUtil.isContinuous(lastOffset, startOffset) && logger.isDebugEnabled()) {
                        logger.debug("Chunk met discontinuous message, pursueTask[{}] baseOffset[{}] nextOffset[{}] runtimes[{}]",
                                pursueTask, lastOffset, startOffset, runtimes);
                    }

                    lastOffset = endOffset;
                    synchronization.dispatchOffset = endOffset;
                    payload = createPayload(startOffset, endOffset, chunk, channel.alloc());
                    count += chunk.count();
                    if (channel.isWritable()) {
                        channel.writeAndFlush(payload.retainedSlice(), channel.voidPromise());
                    } else {
                        pursueTask.setPursueOffset(lastOffset);
                        compute(count);
                        channel.writeAndFlush(payload.retainedSlice(), delayPursue(pursueTask));
                        return;
                    }
                } catch (Exception e) {
                    if (logger.isErrorEnabled()) {
                        logger.error("Chunk pursue failed, pursueTask[{}] lastOffset[{}]", pursueTask, lastOffset, e);
                    }
                } finally {
                    ByteBufUtil.release(chunk.data());
                    ByteBufUtil.release(payload);
                }

                if (runtimes > pursueLimit || chunk.count() <= 1) {
                    finished = false;
                    break;
                }
            }
        } catch (Exception e) {
            if (logger.isErrorEnabled()) {
                logger.error("Chunk pursue failed, pursueTask[{}] lastOffset[{}]", pursueTask, lastOffset, e);
            }
        }

        pursueTask.setPursueOffset(lastOffset);
        compute(count);

        Offset alignOffset = handler.followOffset;
        if (finished || (alignOffset != null && !lastOffset.before(alignOffset))) {
            submitAlign(pursueTask);
        } else {
            submitPursue(pursueTask);
        }
    }

    private void submitFollow(PursueTask<ChunkSynchronization> pursueTask) {
        try {
            channelExecutor(pursueTask.getSubscription().channel).execute(() -> pursueTask.getSubscription().followed = true);
        } catch (Exception e) {
            pursueTask.getSubscription().followed = true;
        }
    }

    private void submitAlign(PursueTask<ChunkSynchronization> pursueTask) {
        try {
            pursueTask.getSubscription().handler.dispatchExecutor.execute(() -> doAlign(pursueTask));
        } catch (Exception e) {
            pursueTask.getSubscription().followed = true;
        }
    }

    private void doAlign(PursueTask<ChunkSynchronization> pursueTask) {
        ChunkSynchronization synchronization = pursueTask.getSubscription();
        Channel channel = synchronization.channel;
        ChunkHandler handler = synchronization.handler;
        if (!channel.isActive() || synchronization != handler.getSubscriptionChannels().get(channel)) {
            return;
        }

        Offset alignOffset = handler.followOffset;
        Offset lastOffset = pursueTask.getPursueOffset();
        if (!lastOffset.before(alignOffset)) {
            synchronization.followed = true;
            return;
        }

        LedgerCursor cursor = pursueTask.getCursor();
        boolean finished = true;
        int count = 0;
        try {
            int runtimes = 0;
            ChunkRecord chunk;
            while ((chunk = cursor.nextChunk(bytesLimit)) != null) {
                runtimes++;
                ByteBuf payload = null;
                try {
                    Offset endOffset = chunk.getEndOffset();
                    if (!endOffset.after(lastOffset)) {
                        if (runtimes > alignLimit) {
                            finished = false;
                            break;
                        }
                        continue;
                    }

                    Offset startOffset = chunk.getStartOffset();
                    if (!MessageUtil.isContinuous(lastOffset, startOffset) && logger.isDebugEnabled()) {
                        logger.debug("Chunk met discontinuous message, pursueTask[{}] baseOffset[{}] nextOffset[{}] runtimes[{}]",
                                pursueTask, lastOffset, startOffset, runtimes);
                    }
                    if (startOffset.after(alignOffset)) {
                        synchronization.followed = true;
                        compute(count);
                        return;
                    }

                    lastOffset = endOffset;
                    synchronization.dispatchOffset = endOffset;
                    payload = createPayload(startOffset, endOffset, chunk, channel.alloc());
                    count += chunk.count();
                    if (channel.isWritable()) {
                        channel.writeAndFlush(payload.retainedSlice(), channel.voidPromise());
                    } else {
                        pursueTask.setPursueOffset(lastOffset);
                        compute(count);
                        channel.writeAndFlush(payload.retainedSlice(), delayPursue(pursueTask));
                        return;
                    }
                } catch (Exception e) {
                    if (logger.isErrorEnabled()) {
                        logger.error("Chunk align failed, pursueTask[{}] lastOffset[{}]", pursueTask, lastOffset, e);
                    }
                } finally {
                    ByteBufUtil.release(chunk.data());
                    ByteBufUtil.release(payload);
                }

                if (runtimes > pursueLimit || chunk.count() <= 1) {
                    finished = false;
                    break;
                }
            }
        } catch (Exception e) {
            if (logger.isErrorEnabled()) {
                logger.error("Chunk align failed, pursueTask[{}] lastOffset[{}]", pursueTask, lastOffset, e);
            }
        }

        if (finished) {
            synchronization.followed = true;
            compute(count);
            return;
        }

        pursueTask.setPursueOffset(lastOffset);
        compute(count);
        submitPursue(pursueTask);
    }

    public void attach(Channel channel, Offset initOffset, Promise<Void> promise) {
        try {
            EventExecutor executor = channelExecutor(channel);
            if (executor.inEventLoop()) {
                doAttach(channel, initOffset, promise);
            } else {
                executor.execute(() -> doAttach(channel, initOffset, promise));
            }
        } catch (Exception e) {
            promise.tryFailure(e);
        }
    }

    private void doAttach(Channel channel, Offset offset, Promise<Void> promise) throws ChunkDispatchException {
        try {
            if (!state.get()) {
                throw new ChunkDispatchException("Chunk dispatcher is inactive");
            }

            ChunkHandler handler = allocateHandler(channel);
            ConcurrentMap<Channel, ChunkSynchronization> channelSynchronizationMap = handler.getSubscriptionChannels();
            ChunkSynchronization oldSynchronization = channelSynchronizationMap.get(channel);
            ChunkSynchronization newSynchronization = new ChunkSynchronization(channel, handler);

            handler.dispatchExecutor.execute(() -> {
                List<ChunkSynchronization> synchronizations = handler.getSynchronizations();
                if (oldSynchronization != null) {
                    synchronizations.remove(oldSynchronization);
                }
                synchronizations.add(newSynchronization);
                if (handler.followCursor == null) {
                    handler.followOffset = storage.currentOffset();
                    handler.followCursor = storage.cursor(handler.followOffset);
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
                } else {
                    dispatchOffset = storage.currentOffset();
                }

                newSynchronization.dispatchOffset = dispatchOffset;
                if (dispatchOffset.before(handler.followOffset)) {
                    PursueTask<ChunkSynchronization> task =
                            new PursueTask<>(newSynchronization, storage.cursor(dispatchOffset), dispatchOffset);
                    submitPursue(task);
                } else {
                    newSynchronization.followed = true;
                }
            });
            channelSynchronizationMap.put(channel, newSynchronization);
            channelHandlers.putIfAbsent(channel, handler);
            promise.trySuccess(null);
        } catch (Exception e) {
            promise.tryFailure(e);
        }
    }

    public void close(Promise<Set<Channel>> promise) {
        Promise<Set<Channel>> result = promise != null ? promise : ImmediateEventExecutor.INSTANCE.newPromise();
        if (state.compareAndSet(true, false)) {
            Set<Channel> channels = new ConcurrentSkipListSet<>();
            AtomicInteger count = new AtomicInteger(executors.length);
            for (EventExecutor executor : executors) {
                try {
                    executor.submit(() -> {
                        for (Channel channel : channelHandlers.keySet()) {
                            if (channelExecutor(channel).inEventLoop()) {
                                channels.add(channel);
                                doCancelSubscribe(channel, ImmediateEventExecutor.INSTANCE.newPromise());
                            }
                        }

                        if (count.decrementAndGet() == 0) {
                            result.trySuccess(channels);
                        }
                    });
                } catch (Exception e) {
                    result.tryFailure(e);
                }
            }
        } else {
            result.trySuccess(null);
        }
    }

    private ChunkHandler allocateHandler(Channel channel) {
        ChunkHandler result = channelHandlers.get(channel);
        if (result != null) {
            return result;
        }
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int middleLimit = loadLimit >> 1;
        synchronized (weakHandlers) {
            if (weakHandlers.isEmpty()) {
                return ChunkHandler.INSTANCE.newHandler(weakHandlers, executors);
            }

            Map<ChunkHandler, Integer> selectHandlers = new HashMap<>();
            int randomBound = 0;
            for (ChunkHandler handler : weakHandlers.keySet()) {
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
                return result != null ? result : ChunkHandler.INSTANCE.newHandler(weakHandlers, executors);
            }

            int index = random.nextInt(randomBound);
            int count = 0;
            for (Map.Entry<ChunkHandler, Integer> entry : selectHandlers.entrySet()) {
                count += loadLimit - entry.getValue();
                if (index < count) {
                    return entry.getKey();
                }
            }
            return result != null ? result : ChunkHandler.INSTANCE.newHandler(weakHandlers, executors);
        }
    }
}
