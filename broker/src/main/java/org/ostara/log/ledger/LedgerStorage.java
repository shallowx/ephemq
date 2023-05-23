package org.ostara.log.ledger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import org.ostara.common.Offset;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import java.util.concurrent.atomic.AtomicBoolean;

public class LedgerStorage {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(LedgerStorage.class);
    private int ledger;
    private String topic;
    private LedgerConfig config;
    private EventExecutor executor;
    private LedgerTrigger trigger;
    private volatile Offset currentOffset;
    private volatile int segmentCount;
    private volatile LedgerSegment headSegment;
    private volatile LedgerSegment tailSegment;
    private AtomicBoolean state = new AtomicBoolean(true);

    public LedgerStorage(int ledger, String topic, int epoch, LedgerConfig config, EventExecutor executor, LedgerTrigger trigger) {
        this.ledger = ledger;
        this.topic = topic;
        this.config = config;
        this.currentOffset = new Offset(epoch, 0L);
        this.executor = executor;
        this.trigger = trigger;
        this.headSegment = this.tailSegment = new LedgerSegment(ledger, Unpooled.EMPTY_BUFFER, currentOffset);
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

    public void doAppendRecord(int marker, ByteBuf payload, Promise<Offset> promise) {
        try{
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
            } catch (Throwable ignored) {}
        }
    }

    public Offset headOffset() {
        return headSegment.baseOffset();
    }

    public LedgerCursor cursor(Offset offset) {
        return offset == null ? tailCursor() : headCursor().seekTo(offset);
    }

    public LedgerCursor headCursor() {
        LedgerSegment theHead = headSegment;
        return new LedgerCursor(this, theHead, theHead.basePosition());
    }

    public LedgerCursor tailCursor() {
        LedgerSegment theTail = tailSegment;
        return new LedgerCursor(this, theTail, theTail.lastPosition());
    }

    private LedgerSegment applySegment(int bytes) {
        LedgerSegment segment = tailSegment;
        if (segment.freeBytes() < bytes) {
            if (segmentCount >= config.segmentRetainCounts()) {
                decreaseSegment();
            }
            return increaseSegment(Math.max(bytes, config.segmentBufferCapacity()));
        }
        return segment;
    }

    private LedgerSegment increaseSegment(int capacity) {
        LedgerSegment theTail = tailSegment;
        ByteBuf buffer = allocateBuffer(capacity);
        LedgerSegment segment = new LedgerSegment(ledger, buffer, theTail.lastOffset());

        theTail.next(segment);
        tailSegment = segment;

        int count = segmentCount;
        if (count == 0) {
            headSegment = segment;
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
            } catch (Throwable ignored){}
        }
    }

    private void doCleanSegment() {
        if (segmentCount == 0) {
            return;
        }

        long timeMillis = System.currentTimeMillis() - config.segmentRetainMs();
        LedgerSegment theBase = headSegment;
        while (true) {
            LedgerSegment theNext = theBase.next();
            if (theNext == null || (theNext.getCreationTime() > timeMillis)) {
                break;
            }

            decreaseSegment();
            theBase = theNext;
        }
    }

    private void decreaseSegment(){
        int count = segmentCount;
        if (count == 0) {
            return;
        }

        LedgerSegment theHead = headSegment;
        if (count > 1) {
            headSegment = theHead.next();
        } else if (count == 1) {
            LedgerSegment empty = new LedgerSegment(ledger, Unpooled.EMPTY_BUFFER, theHead.lastOffset());
            theHead.next(empty);
            headSegment = tailSegment = empty;
        }

        segmentCount = count - 1;
        theHead.release();

        triggerOnRelease(theHead.baseOffset(), headSegment.baseOffset());
    }

    private void triggerOnRelease(Offset oldHead, Offset newHead) {
        if (trigger != null) {
            try {
                trigger.onRelease(ledger, oldHead, newHead);
            } catch (Throwable ignored) {}
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
        return headSegment;
    }

    public LedgerSegment tailSegment() {
        return tailSegment;
    }

    public Offset tailOffset() {
        return tailSegment.lastOffset();
    }

    public long segmentBytes() {
        long bytes = 0L;
        LedgerSegment segment = headSegment;
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
            throw new IllegalStateException(String.format("Ledger storage is inactive, ledger=%d", ledger));
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
               } catch (Throwable t){
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

    private <T>void tryFailure(Promise<T> promise, Throwable t){
        if (promise != null) {
            promise.tryFailure(t);
        }
    }

    private <T>void trySuccess(Promise<T> promise, T v){
        if (promise != null) {
            promise.trySuccess(v);
        }
    }
}

