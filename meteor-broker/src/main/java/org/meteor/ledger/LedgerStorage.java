package org.meteor.ledger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import java.util.concurrent.atomic.AtomicBoolean;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.common.message.Offset;
import org.meteor.common.util.MessageUtil;

public class LedgerStorage {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(LedgerStorage.class);
    private final int ledger;
    private final String topic;
    private final LedgerConfig config;
    private final EventExecutor executor;
    private final LedgerTrigger trigger;
    private final AtomicBoolean state = new AtomicBoolean(true);
    private volatile Offset currentOffset;
    private volatile int segmentCount;
    private volatile LedgerSegment head;
    private volatile LedgerSegment tail;

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

    private void triggerOnAppend(int count, Offset offset) {
        if (trigger != null) {
            try {
                trigger.onAppend(ledger, count, offset);
            } catch (Throwable ignored) {
            }
        }
    }

    public Offset headOffset() {
        return head.baseOffset();
    }

    public LedgerCursor cursor(Offset offset) {
        return offset == null ? tailCursor() : headCursor().seekTo(offset);
    }

    public LedgerCursor headCursor() {
        LedgerSegment theHead = head;
        return new LedgerCursor(this, theHead, theHead.basePosition());
    }

    public LedgerCursor tailCursor() {
        LedgerSegment theTail = tail;
        return new LedgerCursor(this, theTail, theTail.lastPosition());
    }

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

    private void triggerOnRelease(Offset oldHead, Offset newHead) {
        if (trigger != null) {
            try {
                trigger.onRelease(ledger, oldHead, newHead);
            } catch (Throwable t) {
                if (logger.isWarnEnabled()) {
                    logger.warn("[:: topic:{}, old_offset:{}, new_offset:{}]Trigger on release failed", topic, oldHead,
                            newHead);
                }
            }
        }
    }

    private ByteBuf allocateBuffer(int capacity) {
        return PooledByteBufAllocator.DEFAULT.directBuffer(capacity, capacity);
    }

    public int ledger() {
        return ledger;
    }

    public String topic() {
        return topic;
    }

    public LedgerConfig config() {
        return config;
    }

    public Offset currentOffset() {
        return currentOffset;
    }

    public int segmentCount() {
        return segmentCount;
    }

    public LedgerSegment headSegment() {
        return head;
    }

    public LedgerSegment tailSegment() {
        return tail;
    }

    public Offset tailOffset() {
        return tail.lastOffset();
    }

    public long segmentBytes() {
        long bytes = 0L;
        LedgerSegment segment = head;
        while (segment != null) {
            bytes += segment.usedBytes();
            segment = segment.next();
        }

        return bytes;
    }

    public void updateEpoch(int newEpoch) {
        if (executor.inEventLoop()) {
            doUpdateEpoch(newEpoch);
        } else {
            executor.execute(() -> doUpdateEpoch(newEpoch));
        }
    }

    private void doUpdateEpoch(int newEpoch) {
        Offset theOffset = currentOffset;
        if (newEpoch > theOffset.getEpoch()) {
            currentOffset = new Offset(newEpoch, 0L);
        }
    }

    private void checkActive() {
        if (!isActive()) {
            throw new IllegalStateException(STR."Ledger[\{ledger}] storage is inactive");
        }
    }

    public boolean isActive() {
        return state.get();
    }

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

    private void doClose(Promise<Boolean> promise) {
        for (int i = 0; i < segmentCount; i++) {
            decreaseSegment();
        }

        if (promise != null) {
            promise.trySuccess(true);
        }
    }

    private <T> void tryFailure(Promise<T> promise, Throwable t) {
        if (promise != null) {
            promise.tryFailure(t);
        }
    }

    private <T> void trySuccess(Promise<T> promise, T v) {
        if (promise != null) {
            promise.trySuccess(v);
        }
    }

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

    private void doAppendChunkRecord(Channel channel, int count, ByteBuf buf, Promise<Integer> promise) {
        try {
            checkActive();
            int appendCount = 0;
            Offset lastOffset = currentOffset;
            int location = buf.readerIndex();
            final Offset startOffset = new Offset(buf.getInt(location + 8), buf.getLong(location + 12));
            if (logger.isDebugEnabled() && !MessageUtil.isContinuous(lastOffset, startOffset)) {
                logger.debug(
                        "[:: ledger:{}, topic:{}, lastOffset:{}, startOffset:{}]Received append chunk message from {}"
                                + " is discontinuous",
                        ledger, topic, lastOffset, startOffset, channel);
            }
            if (!startOffset.after(lastOffset)) {
                for (int i = 0; i < count; i++) {
                    location = buf.readerIndex();
                    int bytes = buf.getInt(location) + 8;
                    final Offset offset = new Offset(buf.getInt(location + 8), buf.getLong(location + 12));
                    if (!offset.after(lastOffset)) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("[:: offset:{}, lastOffset:{}]Ignore duplicate message", offset, lastOffset);
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

