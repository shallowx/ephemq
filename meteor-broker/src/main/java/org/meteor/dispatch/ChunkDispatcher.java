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

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;

/**
 * The ChunkDispatcher class is responsible for managing the dispatching of
 * data chunks to various channels and handlers, with strict adherence to
 * specified limits and configurations.
 */
public class ChunkDispatcher {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(ChunkDispatcher.class);
    /**
     * The ledger identifier used by the ChunkDispatcher.
     * This variable holds a constant value representing the specific ledger
     * for the chunk dispatching operations within the application.
     */
    private final int ledger;
    /**
     * The topic that this ChunkDispatcher instance is associated with.
     * It represents a unique identifier for a particular stream or set of messages
     * that this dispatcher will handle.
     */
    private final String topic;
    /**
     * Reference to the ledger storage component used for managing storage operations
     * within the ChunkDispatcher class. This is a final variable, meaning its reference
     * cannot be changed once initialized.
     */
    private final LedgerStorage storage;
    /**
     * Represents the maximum number of follow tasks that can be executed simultaneously.
     */
    private final int followLimit;
    /**
     * The maximum number of attempts allowed for chunk pursue operations.
     * This limit dictates how many times the system will attempt to
     * synchronize or catch up with missing chunks before giving up.
     */
    private final int pursueLimit;
    /**
     * The maximum number of times alignment can be attempted for chunk synchronizations.
     * Used in the context of managing offsets and ensuring consistent state between the
     * publisher and subscriber during dispatch operations.
     */
    private final int alignLimit;
    /**
     * The time in milliseconds to wait before timing out a pursue operation.
     * This value is used to configure the pursue behavior within the ChunkDispatcher class.
     */
    private final long pursueTimeoutMilliseconds;
    /**
     * Represents the maximum load limit for the ChunkDispatcher.
     * This variable is used to define the threshold beyond which
     * the load is considered excessive and the system may take
     * appropriate measures to handle it.
     */
    private final int loadLimit;
    /**
     * The limit for the number of bytes that the ChunkDispatcher will pursue
     * during its dispatch operations. This constant helps to ensure that the
     * dispatcher does not exceed a predefined amount of data, which is crucial
     * for maintaining performance and stability.
     */
    private final int pursueBytesLimit;
    /**
     * Represents the byte limit for chunk dispatching operations.
     * This value defines the maximum number of bytes allowed in a single dispatch event.
     */
    private final int bytesLimit;
    /**
     * A consumer that acts on integer values, typically used to count or log events.
     * Used within the context of chunk dispatching to track the number of dispatches or
     * other integer-based metrics.
     */
    private final IntConsumer counter;
    /**
     * An array of {@link EventExecutor} instances used to manage asynchronous tasks
     * for the ChunkDispatcher.
     */
    private final EventExecutor[] executors;
    /**
     * A thread-safe list of ChunkHandlers used for dispatch operations in
     * the ChunkDispatcher.
     * <p>
     * The use of CopyOnWriteArrayList ensures that the list can be safely modified
     * and iterated over concurrently without the need for explicit synchronization.
     * <p>
     * This list is utilized in dispatch processes, where various handlers are involved
     * in managing chunks of data for dispatching.
     */
    private final List<ChunkHandler> dispatchHandlers = new CopyOnWriteArrayList<>();
    /**
     * A map that holds weak references to {@link ChunkHandler} instances, associating them with an {@link Integer} value.
     * Allows for the garbage collection of handlers no longer in use.
     */
    private final WeakHashMap<ChunkHandler, Integer> weakHandlers = new WeakHashMap<>();
    /**
     * A concurrent map that maintains the relationship between a {@link Channel} and its associated {@link ChunkHandler}.
     * <p>
     * This map is used to keep track of active channels and their respective handlers within the {@link ChunkDispatcher}.
     * Each channel is mapped to a unique instance of {@link ChunkHandler} which manages the chunk synchronization tasks
     * specific to that channel.
     */
    private final ConcurrentMap<Channel, ChunkHandler> channelHandlers = new ConcurrentHashMap<>();
    /**
     * The state of the ChunkDispatcher, represented as an AtomicBoolean.
     * It is initialized to true indicating the dispatcher is active.
     */
    private final AtomicBoolean state = new AtomicBoolean(true);

    /**
     * Constructs a ChunkDispatcher instance responsible for managing the dispatch of data chunks
     * from a specified ledger and topic, using various configurations and executor services.
     *
     * @param ledger The identifier of the ledger from which data chunks are dispatched.
     * @param topic The topic associated with the data chunks to be dispatched.
     * @param storage An instance of LedgerStorage that handles the persistence and retrieval of data.
     * @param config Configuration parameters for chunk dispatch behavior.
     * @param executorGroup A group of event executors used to handle asynchronous chunk dispatch tasks.
     * @param dispatchCounter A functional interface that accepts an integer and is used to count dispatch events.
     */
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

    /**
     * Provides the count of channel handlers currently managed by the dispatcher.
     *
     * @return the number of channel handlers
     */
    public int channelCount() {
        return channelHandlers.size();
    }

    /**
     * Selects an EventExecutor for the given channel based on its hash code.
     *
     * @param channel the Channel object for which the EventExecutor needs to be selected
     * @return the selected EventExecutor corresponding to the given channel
     */
    private EventExecutor channelExecutor(Channel channel) {
        return executors[(channel.hashCode() & 0x7fffffff) % executors.length];
    }

    /**
     * Cancels subscriptions for all channels in the current dispatcher.
     * For each channel in the dispatcher, the method uses the immediate event executor
     * to create a new promise and passes the channel and the promise to the
     * {@link #cancelSubscribe(Channel, Promise)} method to handle the cancellation.
     */
    public void cancelSubscribes() {
        for (Channel channel : channelHandlers.keySet()) {
            cancelSubscribe(channel, ImmediateEventExecutor.INSTANCE.newPromise());
        }
    }

    /**
     * Cancels the subscription associated with the given channel.
     *
     * @param channel the channel whose subscription is to be canceled.
     * @param promise the promise to be notified once the subscription is canceled.
     */
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

    /**
     * Cancels the subscription for a given channel.
     *
     * @param channel The channel for which the subscription is being cancelled.
     * @param promise Operation result promise to be completed when the cancellation process is finished.
     */
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

    /**
     * Dispatches chunk handlers for processing.
     * <p>
     * This method iterates over the collection of `dispatchHandlers` and invokes the `touchDispatch`
     * method on each handler that has a non-null `followCursor`. If there are no handlers to process,
     * the method exits early.
     */
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

    /**
     * Triggers the dispatch process for the given ChunkHandler if it has not been triggered yet.
     * The method ensures that the dispatch process is initiated only once by using a compare-and-set operation.
     * If the ChunkHandler is successfully triggered, it schedules the dispatch execution on the handler's executor.
     *
     * @param handler the ChunkHandler that manages the dispatch process and holds the state and resources needed for dispatch
     */
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

    /**
     * Dispatches chunks of data from the ledger to handlers, ensuring data continuity
     * and appropriately handling dispatch offsets and channel writability.
     *
     * @param handler The ChunkHandler responsible for managing the dispatch process.
     */
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
                        logger.debug("[:: handler:{}, baseOffset:{}, nextOffset:{}, runtimes:{}] - Chunk met discontinuous message", handler, lastOffset, startOffset, runTimes);
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
                        logger.error("[:: handler:{}, lastOffset:{}] - Chunk dispatch failed", handler, lastOffset, e);
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
                logger.error("[:: handler:{}, lastOffset:{}] - Chunk dispatch failed", handler, lastOffset, e);
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

    /**
     * Creates a payload byte buffer for a given chunk record within specified start and end offsets.
     *
     * @param startOffset the start offset of the chunk data
     * @param endOffset   the end offset of the chunk data
     * @param chunk       the chunk record containing data to be included in the payload
     * @param alloc       the byte buffer allocator to use for creating the payload
     * @return a ByteBuf containing the constructed payload
     * @throws ChunkDispatchException if an error occurs while building the payload
     */
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
            throw new ChunkDispatchException(
                    STR."[:: ledger:\{ledger}, topic:\{topic}, startOffset:\{startOffset}, ednOffset:\{endOffset}, length:\{chunk.data()
                            .readableBytes()}] - Build payload error");
        }
    }

    /**
     * Processes the given count of chunks and logs any errors that occur during the process.
     *
     * @param count the number of chunks to process. If count is greater than 0, it will be passed to the counter.accept method.
     *              Any exceptions that occur during this process will be logged as errors.
     */
    private void compute(int count) {
        if (count > 0) {
            try {
                counter.accept(count);
            } catch (Exception e) {
                if (logger.isErrorEnabled()) {
                    logger.error("[:: ledger:{}, topic:{}] - Chunk count failed", ledger, topic, e);
                }
            }
        }
    }

    /**
     * Delays the pursuit of a specified task by creating a promise and adding a listener
     * to the promise that submits the task when the associated channel is active.
     *
     * @param pursueTask the task to be pursued, containing the channel subscription, cursor, and offsets information.
     * @return a ChannelPromise representing the delayed operation.
     */
    private ChannelPromise delayPursue(PursueTask<ChunkSynchronization> pursueTask) {
        ChannelPromise promise = pursueTask.getSubscription().channel.newPromise();
        promise.addListener((ChannelFutureListener) f -> {
            if (f.channel().isActive()) {
                submitPursue(pursueTask);
            }
        });
        return promise;
    }

    /**
     * Submits a pursue task to be executed by the appropriate channel executor.
     * If an exception occurs, it falls back to submitting a follow task instead.
     *
     * @param pursueTask the pursue task to be executed, containing the subscription information
     */
    private void submitPursue(PursueTask<ChunkSynchronization> pursueTask) {
        try {
            channelExecutor(pursueTask.getSubscription().channel).execute(() -> doPursue(pursueTask));
        } catch (Exception e) {
            submitFollow(pursueTask);
        }
    }

    /**
     * Executes the pursue operation on the given {@code pursueTask}.
     *
     * @param pursueTask the task containing the synchronization, cursor, and initial pursue offset to process chunk data
     */
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
                        logger.debug(
                                "[:: pursueTask:{}, baseOffset:{}, nextOffset:{}, runtimes:{}] - Chunk met "
                                        + "discontinuous message",
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
                        logger.error("[:: pursueTask:{}, lastOffset:{}] - Chunk pursue failed", pursueTask, lastOffset,
                                e);
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
                logger.error("[:: pursueTask:{}, lastOffset:{}] - Chunk pursue failed", pursueTask, lastOffset, e);
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

    /**
     * Submits a follow task for the provided pursue task.
     *
     * @param pursueTask the pursue task containing the subscription information to be followed
     */
    private void submitFollow(PursueTask<ChunkSynchronization> pursueTask) {
        try {
            channelExecutor(pursueTask.getSubscription().channel).execute(() -> pursueTask.getSubscription().followed = true);
        } catch (Exception e) {
            pursueTask.getSubscription().followed = true;
        }
    }

    /**
     * Submits a task to align the subscription with the current state.
     *
     * @param pursueTask the task containing the subscription and offset to be aligned
     */
    private void submitAlign(PursueTask<ChunkSynchronization> pursueTask) {
        try {
            pursueTask.getSubscription().handler.dispatchExecutor.execute(() -> doAlign(pursueTask));
        } catch (Exception e) {
            pursueTask.getSubscription().followed = true;
        }
    }

    /**
     * Aligns the current state of a `PursueTask` with the state of its associated `ChunkSynchronization`.
     *
     * @param pursueTask the task that contains the `ChunkSynchronization` and the necessary offsets and cursor needed
     *                   to perform the alignment.
     */
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
                        logger.debug(
                                "[:: pursueTask:{}, baseOffset:{}, nextOffset:{}, runtimes:{}] - Chunk met "
                                        + "discontinuous message",
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
                        logger.error("[:: pursueTask:{}, lastOffset:{}] - Chunk align failed", pursueTask, lastOffset,
                                e);
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
                logger.error("[:: pursueTask:{}, lastOffset:{}] - Chunk align failed", pursueTask, lastOffset, e);
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

    /**
     * Attaches a given channel to the dispatcher starting from the specified initial offset.
     *
     * @param channel The channel to attach.
     * @param initOffset The initial offset from where to start.
     * @param promise The promise to be fulfilled upon completion or failure of the attach operation.
     */
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

    /**
     * Attaches a given channel to the chunk dispatcher with a specified offset.
     *
     * @param channel the communication channel to be attached
     * @param offset the initial offset to start dispatching chunks
     * @param promise a promise to track the completion of the attachment
     * @throws ChunkDispatchException if the chunk dispatcher is inactive
     */
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

    /**
     * Closes the dispatcher, optionally handling the provided promise.
     *
     * @param promise A promise to be notified when all channels have been closed.
     *                If null, a new promise will be created and used internally.
     */
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

    /**
     * Allocates a ChunkHandler for a given channel. The handler allocation is based on the load balancing
     * rules and the current state of the weakHandlers. If no suitable existing handler is found, a new handler is created.
     *
     * @param channel The channel for which a ChunkHandler needs to be allocated.
     * @return The allocated ChunkHandler for the provided channel.
     */
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
