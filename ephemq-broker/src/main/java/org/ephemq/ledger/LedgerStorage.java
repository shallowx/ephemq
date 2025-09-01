package org.ephemq.ledger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import org.ephemq.common.logging.InternalLogger;
import org.ephemq.common.logging.InternalLoggerFactory;
import org.ephemq.common.message.Offset;
import org.ephemq.common.util.MessageUtil;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The LedgerStorage class handles the storage and management of ledger segments, allowing records to be appended
 * and retaining offsets for a specific topic.
 */
public class LedgerStorage {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(LedgerStorage.class);
    /**
     * Represents the unique ledger identifier.
     * This is a final variable indicating that it cannot be modified once initialized.
     */
    private final int ledger;
    /**
     * The topic this LedgerStorage instance is associated with.
     * Used for identifying the topic for which ledger operations are performed.
     */
    private final String topic;
    /**
     * The configuration settings for the ledger storage system.
     * This configuration is set during the instantiation of the LedgerStorage
     * and remains constant throughout the lifecycle of the LedgerStorage instance.
     */
    private final LedgerConfig config;
    /**
     * An executor specifically designed to handle event processing. It ensures that
     * the tasks submitted for event handling are executed efficiently and effectively.
     * The executor is immutable and is initialized when the class is instantiated.
     * It handles events in a manner suited to the application's requirements, possibly
     * using underlying thread pools or other concurrency mechanisms.
     */
    private final EventExecutor executor;
    /**
     * The `trigger` represents a final instance of `LedgerTrigger` that is used to define and initiate
     * specific actions or checks within the ledger system.
     * <p>
     * This variable is immutable once initialized, ensuring that the trigger's configuration remains consistent
     * throughout the lifecycle of the application.
     */
    private final LedgerTrigger trigger;
    /**
     * An AtomicBoolean representing the state, initialized to true.
     * It provides a boolean value that may be updated atomically.
     */
    private final AtomicBoolean state = new AtomicBoolean(true);
    /**
     * Represents the current offset of the ledger in the storage system.
     * This variable is used to keep track of the latest position in the ledger,
     * allowing for append operations and segment management.
     * The offset is volatile to ensure visibility across multiple threads.
     */
    private volatile Offset currentOffset;
    /**
     * The `segmentCount` variable holds the total number of segments being processed or managed.
     * This variable is declared as `volatile` to ensure visibility of its updates across threads,
     * providing a mechanism to synchronize its value changes among different threads.
     */
    private volatile int segmentCount;
    /**
     * The head of the ledger segment chain.
     * This variable represents the starting point of the ledger segment list.
     * It is marked as volatile to ensure visibility of updates to this variable
     * across all threads, maintaining data consistency and correctness.
     */
    private volatile LedgerSegment head;
    /**
     * Represents the tail end of the ledger segment within the LedgerStorage system.
     * This variable holds the current last segment in the ledger sequence, facilitating
     * operations related to appending records and managing segment boundaries.
     * The use of the volatile keyword ensures that updates to this variable
     * are immediately visible to other threads.
     */
    private volatile LedgerSegment tail;

    /**
     * Constructs a new LedgerStorage instance.
     *
     * @param ledger    The ID of the ledger.
     * @param topic     The topic associated with the ledger.
     * @param epoch     The epoch for the initial offset.
     * @param config    The configuration settings for the ledger.
     * @param executor  The executor for handling asynchronous events.
     * @param trigger   The trigger for handling ledger events.
     */
    public LedgerStorage(int ledger, String topic, int epoch, LedgerConfig config, EventExecutor executor, LedgerTrigger trigger) {
        this.ledger = ledger;
        this.topic = topic;
        this.config = config;
        this.currentOffset = new Offset(epoch, 0L);
        this.executor = executor;
        this.trigger = trigger;
        if (config.isAlloc()) {
            // For topics with a predictably high message volume, prioritize upfront memory allocation to prevent initial message spikes
            this.head = this.tail = new LedgerSegment(ledger, allocateBuffer(config.segmentBufferCapacity()), currentOffset);
        } else {
            this.head = this.tail = new LedgerSegment(ledger, Unpooled.EMPTY_BUFFER, currentOffset);
        }
    }

    /**
     * Appends a record to the ledger storage. This method retains the payload buffer and ensures
     * that the actual append operation is performed within the appropriate event loop context.
     *
     * @param marker An integer marker indicating the type or nature of the record.
     * @param payload A {@link ByteBuf} containing the record data to be appended.
     * @param promise A {@link Promise} which will be completed with the new {@link Offset} of the appended record or an error if the operation fails.
     */
    public void appendRecord(int marker, ByteBuf payload, Promise<Offset> promise) {
        payload.retain();
        if (executor.inEventLoop()) {
            doAppendRecord(marker, payload, promise);
        } else {
            try {
                executor.execute(() -> doAppendRecord(marker, payload, promise));
            } catch (Throwable t) {
                payload.release();
                tryFailure(promise, t);
            }
        }
    }

    /**
     * Appends a record to the ledger storage.
     *
     * @param marker   an integer marker that may serve as metadata or delimiter indicating a specific point in the stream.
     * @param payload  the data to be written as the record, encapsulated in a ByteBuf object.
     * @param promise  a promise that will be either completed with the offset of the newly appended record
     *                 or failed if an exception occurred during the operation.
     */
    private void doAppendRecord(int marker, ByteBuf payload, Promise<Offset> promise) {
        try {
            checkActive();

            Offset theOffset = currentOffset;
            Offset offset = new Offset(theOffset.getEpoch(), theOffset.getIndex() + 1);
            LedgerSegment segment = applySegment(24 + payload.readableBytes());
            segment.writeRecord(marker, offset, payload);

            currentOffset = offset;
            triggerOnAppend(1, offset);
            trySuccess(promise, offset);
        } catch (Throwable t) {
            tryFailure(promise, t);
        } finally {
            payload.release();
        }
    }

    /**
     * Triggers the append event if the trigger is not null.
     *
     * @param count  the number of records being appended.
     * @param offset the offset of the last appended record.
     */
    private void triggerOnAppend(int count, Offset offset) {
        if (trigger != null) {
            try {
                trigger.onAppend(ledger, count, offset);
            } catch (Throwable ignored) {
            }
        }
    }

    /**
     * Retrieves the head offset of the ledger storage.
     *
     * @return the Offset representing the head position in the ledger storage.
     */
    public Offset headOffset() {
        return head.baseOffset();
    }

    /**
     * Returns a LedgerCursor positioned at the given offset.
     * <p>
     * If the offset is null, the cursor is positioned at the tail of the ledger.
     * Otherwise, the cursor is positioned at the specified offset in the head of the ledger.
     *
     * @param offset the offset to set the cursor position; if null, positions the cursor at the ledger tail
     * @return a LedgerCursor positioned at the specified offset or at the tail if the offset is null
     */
    public LedgerCursor cursor(Offset offset) {
        return offset == null ? tailCursor() : headCursor().seekTo(offset);
    }

    /**
     * Provides a cursor located at the head of the ledger.
     *
     * @return a LedgerCursor object positioned at the head segment's base position of the ledger.
     */
    public LedgerCursor headCursor() {
        LedgerSegment theHead = head;
        return new LedgerCursor(this, theHead, theHead.basePosition());
    }

    /**
     * Returns a {@link LedgerCursor} object positioned at the tail of the ledger.
     *
     * @return a LedgerCursor object positioned at the last position of the tail segment.
     */
    public LedgerCursor tailCursor() {
        LedgerSegment theTail = tail;
        return new LedgerCursor(this, theTail, theTail.lastPosition());
    }

    /**
     * Applies a ledger segment based on the byte requirement.
     *
     * @param bytes the number of bytes required in the ledger segment
     * @return the applicable ledger segment based on the specified byte requirement
     */
    private LedgerSegment applySegment(int bytes) {
        LedgerSegment segment = tail;
        if (segment.freeBytes() < bytes) {
            if (segmentCount >= config.segmentRetainCounts()) {
                decreaseSegment();
            }
            return increaseSegment(Math.max(bytes, config.segmentBufferCapacity()));
        }
        return segment;
    }

    /**
     * Increases the segment capacity in the ledger by creating a new LedgerSegment and linking it
     * to the tail of the current segment list.
     *
     * @param capacity the capacity of the new LedgerSegment to be created
     * @return the newly created LedgerSegment
     */
    private LedgerSegment increaseSegment(int capacity) {
        LedgerSegment theTail = tail;
        ByteBuf buffer = allocateBuffer(capacity);
        LedgerSegment segment = new LedgerSegment(ledger, buffer, theTail.lastOffset());

        theTail.next(segment);
        tail = segment;

        int count = segmentCount;
        if (count == 0) {
            head = segment;
        }

        segmentCount = count + 1;
        return segment;
    }

    /**
     * Initiates the process of cleaning segments in the ledger storage.
     * <p>
     * This method first checks if the execution is in the event loop. If so, it directly proceeds
     * with cleaning the segment by calling {@link #doCleanSegment()}. If not, it attempts to
     * execute the cleaning task in the event loop using the executor.
     * <p>
     * The method is designed to handle potential exceptions silently that may occur during
     * the execution of tasks on the executor.
     */
    public void cleanSegment() {
        if (executor.inEventLoop()) {
            doCleanSegment();
        } else {
            try {
                executor.execute(this::cleanSegment);
            } catch (Throwable ignored) {
            }
        }
    }

    /**
     * Cleans up old ledger segments that exceed the configured retention time.
     * <p>
     * This method iterates through the ledger segments starting from the head and
     * removes segments that are older than the configured retention time defined
     * by {@code config.segmentRetainMs()}. The segments are removed by calling
     * {@code decreaseSegment()} method until a segment is found that is within the
     * retention time or there are no more segments left.
     * <p>
     * If there are no segments to begin with (i.e., {@code segmentCount == 0}),
     * this method returns immediately.
     */
    private void doCleanSegment() {
        if (segmentCount == 0) {
            return;
        }

        long timeMillis = System.currentTimeMillis() - config.segmentRetainMs();
        LedgerSegment theBase = head;
        while (true) {
            LedgerSegment theNext = theBase.next();
            if (theNext == null || (theNext.getCreationTime() > timeMillis)) {
                break;
            }

            decreaseSegment();
            theBase = theNext;
        }
    }

    /**
     * Decreases the number of segments in the ledger by one.
     * <p>
     * If there are no segments, the method returns immediately.
     * If the segment count is greater than one, the head segment is shifted to the next segment.
     * If there is only one segment, a new empty segment is created and assigned as both the head and tail.
     * <p>
     * The segment count is then decremented, and the released segment is deallocated.
     * Finally, the triggerOnRelease method is invoked to perform any additional operations related to the segment release.
     */
    private void decreaseSegment() {
        int count = segmentCount;
        if (count == 0) {
            return;
        }

        LedgerSegment theHead = head;
        if (count > 1) {
            head = theHead.next();
        } else if (count == 1) {
            LedgerSegment empty = new LedgerSegment(ledger, Unpooled.EMPTY_BUFFER, theHead.lastOffset());
            theHead.next(empty);
            head = tail = empty;
        }

        segmentCount = count - 1;
        theHead.release();

        triggerOnRelease(theHead.baseOffset(), head.baseOffset());
    }

    /**
     * Triggers release action when the head offset changes.
     *
     * @param oldHead the previous head offset
     * @param newHead the new head offset
     */
    private void triggerOnRelease(Offset oldHead, Offset newHead) {
        if (trigger != null) {
            try {
                trigger.onRelease(ledger, oldHead, newHead);
            } catch (Throwable t) {
                if (logger.isWarnEnabled()) {
                    logger.warn("Trigger on release failed - [topic:{}, old_offset:{}, new_offset:{}]", topic, oldHead, newHead);
                }
            }
        }
    }

    /**
     * Allocates a direct buffer with the specified capacity using the default pooled byte buffer allocator.
     *
     * @param capacity the capacity of the buffer to allocate
     * @return a newly allocated direct buffer
     */
    private ByteBuf allocateBuffer(int capacity) {
        return PooledByteBufAllocator.DEFAULT.directBuffer(capacity, capacity);
    }

    /**
     * Retrieves the current value of the ledger.
     *
     * @return the integer value representing the current state of the ledger
     */
    public int ledger() {
        return ledger;
    }

    /**
     * Retrieves the current topic.
     *
     * @return the current topic as a String.
     */
    public String topic() {
        return topic;
    }

    /**
     * Returns the {@code LedgerConfig} associated with this ledger storage.
     *
     * @return the current {@code LedgerConfig} instance.
     */
    public LedgerConfig config() {
        return config;
    }

    /**
     * Returns the current offset of the ledger storage.
     *
     * @return the current offset where new records can be appended in the ledger.
     */
    public Offset currentOffset() {
        return currentOffset;
    }

    /**
     * Returns the number of segments in the ledger storage.
     *
     * @return the current number of segments.
     */
    public int segmentCount() {
        return segmentCount;
    }

    /**
     * Retrieves the head segment of the ledger.
     *
     * @return the head segment of the ledger.
     */
    public LedgerSegment headSegment() {
        return head;
    }

    /**
     * Returns the current tail segment of the ledger storage.
     *
     * @return the tail segment of the ledger storage
     */
    public LedgerSegment tailSegment() {
        return tail;
    }

    /**
     * Retrieves the last offset from the tail segment of the ledger storage.
     *
     * @return the last offset in the tail ledger segment.
     */
    public Offset tailOffset() {
        return tail.lastOffset();
    }

    /**
     * Calculates the total number of bytes used across all ledger segments.
     *
     * @return the total number of bytes used by all segments.
     */
    public long segmentBytes() {
        long bytes = 0L;
        LedgerSegment segment = head;
        while (segment != null) {
            bytes += segment.usedBytes();
            segment = segment.next();
        }

        return bytes;
    }

    /**
     * Updates the epoch to the specified new value.
     * <p>
     * If the current thread is in the event loop, it directly updates the epoch.
     * Otherwise, it schedules the epoch update to be executed in the event loop.
     *
     * @param newEpoch the new epoch value to be set
     */
    public void updateEpoch(int newEpoch) {
        if (executor.inEventLoop()) {
            doUpdateEpoch(newEpoch);
        } else {
            executor.execute(() -> doUpdateEpoch(newEpoch));
        }
    }

    /**
     * Updates the current epoch of the ledger if the provided epoch is greater than the existing one.
     *
     * @param newEpoch the new epoch value to be set
     */
    private void doUpdateEpoch(int newEpoch) {
        Offset theOffset = currentOffset;
        if (newEpoch > theOffset.getEpoch()) {
            currentOffset = new Offset(newEpoch, 0L);
        }
    }

    /**
     * Checks if the storage is active. If the storage is inactive, it throws an
     * IllegalStateException.
     * <p>
     * This is a private method used internally to ensure that operations are
     * not performed on an inactive storage.
     *
     * @throws IllegalStateException if the storage is inactive.
     */
    private void checkActive() {
        if (!isActive()) {
            throw new IllegalStateException(String.format("storage is inactive - ledger[%sd)", ledger));
        }
    }

    /**
     * Checks if the current state is active.
     *
     * @return {@code true} if the current state is active, {@code false} otherwise.
     */
    public boolean isActive() {
        return state.get();
    }

    /**
     * Closes the LedgerStorage and returns a Future indicating the success or failure of the operation.
     *
     * @param promise A Promise object that will be completed with the result of the close operation.
     *                If null, a new promise will be created.
     * @return A Future containing a Boolean value indicating whether the close operation was successful.
     */
    public Future<Boolean> close(Promise<Boolean> promise) {
        Promise<Boolean> result = promise != null ? promise : ImmediateEventExecutor.INSTANCE.newPromise();
        if (state.compareAndSet(true, false)) {
            if (executor.inEventLoop()) {
                doClose(promise);
            } else {
                try {
                    executor.execute(() -> doClose(promise));
                } catch (Throwable t) {
                    result.tryFailure(t);
                }
            }
        } else {
            result.trySuccess(true);
        }
        return result;
    }

    /**
     * Closes the current ledger storage by decreasing all segments and fulfilling the promise.
     *
     * @param promise a Promise of type Boolean, which will be set to true if the operation is successful
     */
    private void doClose(Promise<Boolean> promise) {
        for (int i = 0; i < segmentCount; i++) {
            decreaseSegment();
        }

        if (promise != null) {
            promise.trySuccess(true);
        }
    }

    /**
     * Attempts to set the given {@link Promise} to a failure state with the provided {@link Throwable}.
     * If the promise is not null, it will be transitioned to a failure state by this method.
     *
     * @param <T> The type parameter of the {@link Promise}.
     * @param promise The promise to be set to a failure state. Can be null.
     * @param t The throwable representing the failure cause.
     */
    private <T> void tryFailure(Promise<T> promise, Throwable t) {
        if (promise != null) {
            promise.tryFailure(t);
        }
    }

    /**
     * Attempts to mark the specified promise as successful with the provided value.
     *
     * @param <T> the type of the value being set for the promise
     * @param promise the promise to be marked as successful, if it is not null
     * @param v the value to set for the promise if it is not null
     */
    private <T> void trySuccess(Promise<T> promise, T v) {
        if (promise != null) {
            promise.trySuccess(v);
        }
    }

    /**
     * Appends a chunk record to the ledger storage. If the current thread is in an event loop, it executes the chunk append directly.
     * Otherwise, it submits the chunk append task to the event executor's queue.
     *
     * @param channel the channel through which the record is received.
     * @param count the number of records to be appended.
     * @param buf the buffer containing the record data.
     * @param promise the promise to be completed once the append operation is finished.
     */
    public void appendChunkRecord(Channel channel, int count, ByteBuf buf, Promise<Integer> promise) {
        buf.retain();
        if (executor.inEventLoop()) {
            doAppendChunkRecord(channel, count, buf, promise);
        } else {
            try {
                executor.submit(() -> doAppendChunkRecord(channel, count, buf, promise));
            } catch (Throwable t) {
                buf.release();
                promise.tryFailure(t);
            }
        }
    }

    /**
     * Appends a chunk of records to the ledger storage.
     *
     * @param channel the channel through which records are received
     * @param count the number of records to append
     * @param buf the buffer containing the chunk of records
     * @param promise a promise to be fulfilled with the number of appended records or an error
     */
    private void doAppendChunkRecord(Channel channel, int count, ByteBuf buf, Promise<Integer> promise) {
        try {
            checkActive();
            int appendCount = 0;
            Offset lastOffset = currentOffset;
            int location = buf.readerIndex();
            final Offset startOffset = new Offset(buf.getInt(location + 8), buf.getLong(location + 12));
            if (logger.isDebugEnabled() && !MessageUtil.isContinuous(lastOffset, startOffset)) {
                logger.debug(
                        "Received append chunk message from channel[{}] is discontinuous - [ledger:{}, topic:{}, lastOffset:{}, startOffset:{}]", channel, ledger, topic, lastOffset, startOffset);
            }
            if (!startOffset.after(lastOffset)) {
                for (int i = 0; i < count; i++) {
                    location = buf.readerIndex();
                    int bytes = buf.getInt(location) + 8;
                    final Offset offset = new Offset(buf.getInt(location + 8), buf.getLong(location + 12));
                    if (!offset.after(lastOffset)) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Ignore duplicate message - [offset:{}, lastOffset:{}]", offset, lastOffset);
                        }
                        buf.skipBytes(bytes);
                        continue;
                    }

                    LedgerSegment segment = applySegment(bytes);
                    segment.writeChunkRecord(bytes, offset, buf);
                    currentOffset = offset;
                    lastOffset = offset;
                    appendCount++;
                }
            } else {
                int bytes = buf.readableBytes();
                int endLocation = location + bytes;
                int lastRecordLength = buf.getInt(endLocation - 4);
                final Offset endOffset = new Offset(buf.getInt(endLocation - lastRecordLength), buf.getLong(endLocation - lastRecordLength) + 4);
                LedgerSegment segment = applySegment(bytes);
                segment.writeChunkRecord(bytes, endOffset, buf);
                currentOffset = endOffset;
                lastOffset = endOffset;
                appendCount += count;
                if (appendCount > 0) {
                    triggerOnAppend(appendCount, lastOffset);
                }
                promise.trySuccess(appendCount);
            }
        } catch (Exception e) {
            promise.tryFailure(e);
        } finally {
            buf.release();
        }
    }
}

