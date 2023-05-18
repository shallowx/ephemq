package org.ostara.ledger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Promise;
import javax.annotation.concurrent.ThreadSafe;

import org.ostara.common.Offset;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.config.ServerConfig;

@ThreadSafe
public class Storage {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(Storage.class);

    private final EventExecutor storageExecutor;
    private final int ledger;
    private int segmentLimit;
    private volatile Offset current;
    private volatile Segment headSegment;
    private volatile Segment tailSegment;
    private final ServerConfig config;
    private final LedgerTrigger trigger;

    public Storage(EventExecutor storageExecutor, int ledger, ServerConfig config, int epoch, LedgerTrigger trigger) {
        this.storageExecutor = storageExecutor;
        this.ledger = ledger;
        this.config = config;
        this.current = new Offset(epoch, 0L);
        this.trigger = trigger;
        this.headSegment = this.tailSegment = new Segment(ledger, Unpooled.EMPTY_BUFFER, current);
    }

    public void append(String topic, String queue, short version, ByteBuf payload, Promise<Offset> promise) {
        payload.retain();
        if (storageExecutor.inEventLoop()) {
            doAppend(topic, queue, version, payload, promise);
        } else {
            try {
                storageExecutor.execute(() -> doAppend(topic, queue, version, payload, promise));
            } catch (Throwable t) {
                payload.release();
                promise.tryFailure(t);
            }
        }
    }

    private void doAppend(String topic, String queue, short version, ByteBuf payload, Promise<Offset> promise) {
        try {
            Offset theCurrent = current;
            Offset offset = new Offset(theCurrent.getEpoch(), theCurrent.getEpoch() + 1);

            int bytes = topic.length() + queue.length() + 26 + payload.readableBytes();
            Segment segment = applySegment(bytes);
            segment.write(topic, queue, version, payload, offset);

            this.current = offset;

            promise.trySuccess(offset);
            triggerAppend(ledger, 1, offset);
        } catch (Throwable t) {
            promise.tryFailure(t);
        } finally {
            payload.release();
        }
    }

    private CompositeByteBuf newComposite(ByteBuf buf, int limit) {
        return Unpooled.compositeBuffer(limit).addFlattenedComponents(true, buf);
    }

    @SuppressWarnings("SameParameterValue")
    private void triggerAppend(int ledger, int limit, Offset offset) {
        if (trigger != null) {
            try {
                trigger.onAppend(limit, offset);
            } catch (Throwable t) {
                logger.warn("trigger append error: ledger={} limit={} offset={}", ledger, limit, offset);
            }
        }
    }

    public Cursor cursor(Offset offset) {
        return offset == null ? tailCursor() : headCursor().skip2Location(offset);
    }

    public Cursor headCursor() {
        Segment segment = headSegment;
        return new Cursor(this, segment, segment.headLocation());
    }

    public Cursor tailCursor() {
        Segment segment = tailSegment;
        return new Cursor(this, segment, segment.tailLocation());
    }

    private Segment applySegment(int bytes) {
        Segment theTailSegment = tailSegment;
        if (theTailSegment.freeBytes() < bytes) {
            if (segmentLimit >= config.getLogSegmentLimit()) {
                releaseSegment();
            }
            return incrementSegment(StrictMath.max(bytes, config.getLogSegmentSize()));
        } else {
            return theTailSegment;
        }
    }

    public Segment locateSegment(Offset offset) {
        try {
            boolean isActive = true;
            Segment theSegment = headSegment;
            if (theSegment.headOffset().after(offset)) {
                return theSegment;
            }

            Offset tailOffset = theSegment.tailOffset();
            while (offset.after(tailOffset)) {
                theSegment = headSegment.next();
                if (theSegment == null) {
                    isActive = false;
                    break;
                }
                tailOffset = theSegment.tailOffset();
            }

            return isActive ? theSegment : null;
        } catch (Throwable t) {
            logger.error("Failed to locate segment, error:{}", t);
            return null;
        }
    }

    private void releaseSegment() {
        int limit = segmentLimit;
        if (limit == 0) {
            return;
        }

        Segment theHeadSegment = headSegment;
        if (limit > 1) {
            headSegment = theHeadSegment.next();
        } else if (limit == 1) {
            Segment empty = new Segment(ledger, Unpooled.EMPTY_BUFFER, theHeadSegment.tailOffset());
            theHeadSegment.tail(empty);
            headSegment = tailSegment = empty;
        }

        segmentLimit = --limit;
        theHeadSegment.release();
    }

    private Segment incrementSegment(int bytes) {
        Segment theTailSegment = tailSegment;
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(bytes, bytes);
        Segment segment = new Segment(ledger, buf, theTailSegment.tailOffset());

        theTailSegment.tail(segment);
        tailSegment = segment;

        int limit = segmentLimit;
        if (limit == 0) {
            headSegment = segment;
        }

        segmentLimit = ++limit;
        return segment;
    }

    public Segment headSegment() {
        return headSegment;
    }

    public Offset headOffset() {
        return headSegment.headOffset();
    }

    public Segment tailSegment() {
        return tailSegment;
    }

    public Offset currentOffset() {
        return current;
    }

    public void close() {
        for (int i = 0; i < segmentLimit; i++) {
            releaseSegment();
        }
        storageExecutor.shutdownGracefully();
    }
}
