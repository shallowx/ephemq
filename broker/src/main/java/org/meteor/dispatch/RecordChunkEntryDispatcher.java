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
import org.meteor.common.message.Offset;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.common.util.MessageUtil;
import org.meteor.config.ChunkRecordDispatchConfig;
import org.meteor.ledger.ChunkRecord;
import org.meteor.ledger.LedgerCursor;
import org.meteor.ledger.LedgerStorage;
import org.meteor.remote.codec.MessagePacket;
import org.meteor.remote.processor.ProcessCommand;
import org.meteor.remote.proto.client.SyncMessageSignal;
import org.meteor.remote.util.ByteBufUtil;
import org.meteor.remote.util.ProtoBufUtil;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;

public class RecordChunkEntryDispatcher {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(RecordEntryDispatcher.class);

    private final int ledger;
    private final String topic;
    private final LedgerStorage storage;
    private final int followLimit;
    private final int pursueLimit;
    private final int alignLimit;
    private final long pursueTimeoutMs;
    private final int loadLimit;
    private final int pursueBytesLimit;
    private final int bytesLimit;
    private final IntConsumer counter;
    private final EventExecutor[] executors;
    private final List<RecordChunkHandler> dispatchHandlers = new CopyOnWriteArrayList<>();
    private final WeakHashMap<RecordChunkHandler, Integer> allocateHandlers = new WeakHashMap<>();
    private final ConcurrentMap<Channel, RecordChunkHandler> channelHandlerMap = new ConcurrentHashMap<>();
    private final AtomicBoolean state = new AtomicBoolean(true);

    public RecordChunkEntryDispatcher(int ledger, String topic, LedgerStorage storage, ChunkRecordDispatchConfig config, EventExecutorGroup executorGroup, IntConsumer dispatchCounter) {
        this.ledger = ledger;
        this.topic = topic;
        this.storage = storage;
        this.followLimit = config.getChunkDispatchEntryFollowLimit();
        this.pursueLimit = config.getChunkDispatchEntryPursueLimit();
        this.alignLimit = config.getChunkDispatchEntryAlignLimit();
        this.pursueTimeoutMs = config.getChunkDispatchEntryPursueTimeoutMs();
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
        return channelHandlerMap.size();
    }

    private EventExecutor channelExecutor(Channel channel) {
        return executors[(channel.hashCode() & 0x7fffffff) % executors.length];
    }

    public void deSubscribeAll() {
        for (Channel channel : channelHandlerMap.keySet()) {
            deSubscribe(channel, ImmediateEventExecutor.INSTANCE.newPromise());
        }
    }

    public void deSubscribe(Channel channel, Promise<Void> promise) {
        try {
            EventExecutor executor = channelExecutor(channel);
            if (executor.inEventLoop()) {
                doDeSubscribe(channel, promise);
            } else {
                executor.execute(() -> doDeSubscribe(channel, promise));
            }
        } catch (Exception e) {
            promise.tryFailure(e);
        }
    }

    private void doDeSubscribe(Channel channel, Promise<Void> promise) {
        try {
            if (!state.get()) {
                throw new IllegalStateException("Chunk dispatcher is inactive");
            }
            RecordChunkHandler handler = channelHandlerMap.get(channel);
            if (handler == null) {
                promise.trySuccess(null);
                return;
            }
            ConcurrentMap<Channel, RecordChunkSynchronization> channelSynchronizationMap = handler.getChannelSubscriptionMap();
            RecordChunkSynchronization synchronization = channelSynchronizationMap.get(channel);
            if (synchronization == null) {
                promise.trySuccess(null);
                return;
            }

            handler.dispatchExecutor.execute(() -> {
                List<RecordChunkSynchronization> synchronizations = handler.getSynchronizations();
                synchronizations.remove(synchronization);
                if (synchronizations.isEmpty()) {
                    dispatchHandlers.remove(handler);
                    handler.followCursor = null;
                    handler.followOffset = null;
                }
            });

            channelSynchronizationMap.remove(channel);
            channelHandlerMap.remove(channel);
            promise.trySuccess(null);
        } catch (Exception e) {
            promise.tryFailure(e);
        }
    }

    public void dispatch() {
        if (dispatchHandlers.isEmpty()) {
            return;
        }
        for (RecordChunkHandler handler : dispatchHandlers) {
            if (handler.followCursor != null) {
                touchDispatch(handler);
            }
        }
    }

    private void touchDispatch(RecordChunkHandler handler) {
        if (handler.triggered.compareAndSet(false,true)) {
            try {
                handler.dispatchExecutor.execute(() -> doDispatch(handler));
            } catch (Exception e) {
                logger.error("Chunk submit dispatch failed", e);
            }
        }
    }

    private void doDispatch(RecordChunkHandler handler) {
        LedgerCursor cursor = handler.followCursor;
        if (cursor == null) {
            handler.triggered.set(false);
            return;
        }

        List<RecordChunkSynchronization> synchronizations = handler.getSynchronizations();
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
                    if (!MessageUtil.isContinuous(lastOffset, startOffset)) {
                        logger.warn("Chunk met discontinuous message, {} baseOffset={} nextOffset={} runtimes={}",
                                handler, lastOffset, startOffset, runTimes);
                    }

                    lastOffset = endOffset;
                    for (RecordChunkSynchronization synchronization : synchronizations) {
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
                            payload = constructPayload(startOffset, endOffset, chunk, channel.alloc());
                        }

                        count += chunk.count();
                        if (channel.isWritable()) {
                            channel.writeAndFlush(payload.retainedDuplicate(), channel.voidPromise());
                        } else {
                            synchronization.followed = true;
                            PursueTask<RecordChunkSynchronization> pursueTask = new PursueTask<>(synchronization, cursor.copy(), endOffset);
                            channel.writeAndFlush(payload.retainedDuplicate(), delayPursue(pursueTask));
                        }
                    }
                } catch (Exception e){
                    logger.error("chunk dispatch failed, {} lastOffset={}", handler, lastOffset, e);
                } finally {
                    ByteBufUtil.release(chunk.data());
                    ByteBufUtil.release(payload);
                }

                if (runTimes > followLimit || chunk.count() <= 1){
                    break;
                }
            }
        } catch (Exception e){
            logger.error("chunk dispatch failed, {} lastOffset={}", handler, lastOffset, e);
        } finally {
            handler.triggered.set(false);
        }

        handler.followOffset = lastOffset;
        countMessage(count);
        if (cursor.hashNext()) {
            touchDispatch(handler);
        }
    }

    private ByteBuf constructPayload(Offset startOffset, Offset endOffset, ChunkRecord chunk, ByteBufAllocator alloc) {
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
            buf.writeInt(ProcessCommand.Client.SYNC_MESSAGE);
            buf.writeInt(0);
            ProtoBufUtil.writeProto(buf, signal);
            buf = Unpooled.wrappedUnmodifiableBuffer(buf, data.retainedSlice());
            return buf;
        } catch (Exception e) {
            ByteBufUtil.release(buf);
            throw new RuntimeException(String.format(
                    "Build payload error, ledger=%d topic=%s startOffset=%s ednOffset=%s length=%d",
                    ledger, topic, startOffset, endOffset, chunk.data().readableBytes()
            ));
        }
    }

    private void countMessage(int count) {
        if (count > 0) {
            try {
                counter.accept(count);
            } catch (Exception e) {
                logger.error("chunk count failed, ledger={} topic={}", ledger, topic, e);
            }
        }
    }

    private ChannelPromise delayPursue(PursueTask<RecordChunkSynchronization> pursueTask) {
        ChannelPromise promise = pursueTask.getSubscription().channel.newPromise();
        promise.addListener((ChannelFutureListener) f -> {
            if (f.channel().isActive()) {
                submitPursue(pursueTask);
            }
        });
        return promise;
    }

    private void submitPursue(PursueTask<RecordChunkSynchronization> pursueTask) {
        try {
            channelExecutor(pursueTask.getSubscription().channel).execute(() -> doPursue(pursueTask));
        } catch (Exception e){
            submitFollow(pursueTask);
        }
    }

    private void doPursue(PursueTask<RecordChunkSynchronization> pursueTask) {
        RecordChunkSynchronization synchronization = pursueTask.getSubscription();
        Channel channel = synchronization.channel;
        RecordChunkHandler handler = synchronization.handler;
        if (!channel.isActive() || synchronization != handler.getChannelSubscriptionMap().get(channel)) {
            return;
        }

        if (System.currentTimeMillis() - pursueTask.getPursueTime() > pursueTimeoutMs) {
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
                    if (!MessageUtil.isContinuous(lastOffset, startOffset)) {
                        logger.warn("Chunk met discontinuous message, {} baseOffset={} nextOffset={} runtimes={}",
                                pursueTask, lastOffset, startOffset, runtimes);
                    }

                    lastOffset = endOffset;
                    synchronization.dispatchOffset = endOffset;
                    payload = constructPayload(startOffset, endOffset, chunk, channel.alloc());
                    count += chunk.count();
                    if (channel.isWritable()) {
                        channel.writeAndFlush(payload.retainedSlice(), channel.voidPromise());
                    } else {
                        pursueTask.setPursueOffset(lastOffset);
                        countMessage(count);
                        channel.writeAndFlush(payload.retainedSlice(), delayPursue(pursueTask));
                        return;
                    }
                }catch (Exception e) {
                    logger.error("chunk pursue failed, {} lastOffset={}", pursueTask, lastOffset, e);
                } finally {
                    ByteBufUtil.release(chunk.data());
                    ByteBufUtil.release(payload);
                }

                if (runtimes > pursueLimit || chunk.count() <= 1) {
                    finished = false;
                    break;
                }
            }
        } catch (Exception e){
            logger.error("chunk pursue failed, {} lastOffset={}", pursueTask, lastOffset, e);
        }

        pursueTask.setPursueOffset(lastOffset);
        countMessage(count);

        Offset alignOffset = handler.followOffset;
        if (finished || (alignOffset != null && !lastOffset.before(alignOffset))) {
            submitAlign(pursueTask);
        } else {
            submitPursue(pursueTask);
        }
    }

    private void submitFollow(PursueTask<RecordChunkSynchronization> pursueTask) {
        try {
            channelExecutor(pursueTask.getSubscription().channel).execute(() -> pursueTask.getSubscription().followed = true);
        } catch (Exception e) {
            pursueTask.getSubscription().followed = true;
        }
    }

    private void submitAlign(PursueTask<RecordChunkSynchronization> pursueTask) {
        try {
           pursueTask.getSubscription().handler.dispatchExecutor.execute(() -> doAlign(pursueTask));
        } catch (Exception e) {
            pursueTask.getSubscription().followed = true;
        }
    }

    private void doAlign(PursueTask<RecordChunkSynchronization> pursueTask) {
        RecordChunkSynchronization synchronization = pursueTask.getSubscription();
        Channel channel = synchronization.channel;
        RecordChunkHandler handler = synchronization.handler;
        if (!channel.isActive() || synchronization != handler.getChannelSubscriptionMap().get(channel)) {
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
                    if (!MessageUtil.isContinuous(lastOffset, startOffset)) {
                        logger.warn("Chunk met discontinuous message, {} baseOffset={} nextOffset={} runtimes={}",
                                pursueTask, lastOffset, startOffset, runtimes);
                    }
                    if (startOffset.after(alignOffset)) {
                        synchronization.followed = true;
                        countMessage(count);
                        return;
                    }

                    lastOffset = endOffset;
                    synchronization.dispatchOffset = endOffset;
                    payload = constructPayload(startOffset, endOffset, chunk, channel.alloc());
                    count += chunk.count();
                    if (channel.isWritable()) {
                        channel.writeAndFlush(payload.retainedSlice(), channel.voidPromise());
                    }else {
                        pursueTask.setPursueOffset(lastOffset);
                        countMessage(count);
                        channel.writeAndFlush(payload.retainedSlice(), delayPursue(pursueTask));
                        return;
                    }
                } catch (Exception e){
                    logger.error("chunk align failed, {} lastOffset={}", pursueTask, lastOffset, e);
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
            logger.error("chunk align failed, {} lastOffset={}", pursueTask, lastOffset, e);
        }

        if (finished) {
            synchronization.followed = true;
            countMessage(count);
            return;
        }

        pursueTask.setPursueOffset(lastOffset);
        countMessage(count);
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

    private void doAttach(Channel channel, Offset offset, Promise<Void> promise) {
        try {
            if (!state.get()) {
                throw new IllegalStateException("Chunk dispatcher is inactive");
            }

            RecordChunkHandler handler = allocateHandler(channel);
            ConcurrentMap<Channel, RecordChunkSynchronization> channelSynchronizationMap = handler.getChannelSubscriptionMap();
            RecordChunkSynchronization oldSynchronization = channelSynchronizationMap.get(channel);
            RecordChunkSynchronization newSynchronization = new RecordChunkSynchronization(channel, handler);

            handler.dispatchExecutor.execute(() -> {
                List<RecordChunkSynchronization> synchronizations = handler.getSynchronizations();
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
                    PursueTask task = new PursueTask(newSynchronization, storage.cursor(dispatchOffset), dispatchOffset);
                    submitPursue(task);
                } else {
                    newSynchronization.followed = true;
                }
            });
            channelSynchronizationMap.put(channel, newSynchronization);
            channelHandlerMap.putIfAbsent(channel, handler);
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
                        for (Channel channel : channelHandlerMap.keySet()) {
                            if (channelExecutor(channel).inEventLoop()) {
                                channels.add(channel);
                                doDeSubscribe(channel, ImmediateEventExecutor.INSTANCE.newPromise());
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

    private RecordChunkHandler allocateHandler(Channel channel) {
        RecordChunkHandler result = channelHandlerMap.get(channel);
        if (result != null) {
            return result;
        }
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int middleLimit = loadLimit >> 1;
        synchronized (allocateHandlers) {
            if (allocateHandlers.isEmpty()) {
                return RecordChunkHandler.INSTANCE.newHandler(allocateHandlers, executors);
            }

            Map<RecordChunkHandler, Integer> selectHandlers = new HashMap<>();
            int randomBound = 0;
            for (RecordChunkHandler handler : allocateHandlers.keySet()) {
                int channelCount = handler.getChannelSubscriptionMap().size();
                if (channelCount >= loadLimit) {
                    continue;
                }

                if (channelCount >= middleLimit) {
                    randomBound += loadLimit - channelCount;
                    selectHandlers.put(handler, channelCount);
                } else if (result == null || result.getChannelSubscriptionMap().size() < channelCount) {
                    result = handler;
                }
            }

            if (selectHandlers.isEmpty() || randomBound == 0) {
                return result != null ? result : RecordChunkHandler.INSTANCE.newHandler(allocateHandlers, executors);
            }

            int index = random.nextInt(randomBound);
            int count = 0;
            for (Map.Entry<RecordChunkHandler, Integer> entry : selectHandlers.entrySet()) {
                count += loadLimit - entry.getValue();
                if (index < count) {
                    return entry.getKey();
                }
            }
            return result != null ? result : RecordChunkHandler.INSTANCE.newHandler(allocateHandlers, executors);
        }
    }
}
