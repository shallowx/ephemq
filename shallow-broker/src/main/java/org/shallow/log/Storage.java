package org.shallow.log;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Promise;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import static org.shallow.util.ObjectUtil.isNotNull;

@ThreadSafe
public class Storage {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(Storage.class);

    private final EventExecutor storageExecutor;
    private final int ledger;
    private int segmentLimit;
    private volatile Offset current;
    private volatile Segment headSegment;
    private volatile Segment tailSegment;
    private final BrokerConfig config;
    private final LedgerTrigger trigger;

    public Storage(EventExecutor storageExecutor, int ledger, BrokerConfig config, int epoch, LedgerTrigger trigger) {
        this.storageExecutor = storageExecutor;
        this.ledger = ledger;
        this.config = config;
        this.current = new Offset(epoch, 0L);
        this.trigger = trigger;
        this.headSegment = this.tailSegment = new Segment(ledger, Unpooled.EMPTY_BUFFER, current);
    }

    public void append(String queue, ByteBuf payload, Promise<Offset> promise) {
        payload.retain();
        if (storageExecutor.inEventLoop()) {
            doAppend(queue, payload, promise);
        } else {
            try {
                storageExecutor.execute(() -> doAppend(queue, payload, promise));
            } catch (Throwable t) {
                payload.release();
                promise.tryFailure(t);
            }
        }
    }

    private void doAppend(String queue, ByteBuf payload, Promise<Offset> promise) {
        try {
            Offset theCurrent = current;
            Offset offset = new Offset(theCurrent.epoch(), theCurrent.index() + 1);

            int bytes = queue.length() + 20 + payload.readableBytes();
            Segment segment = applySegment(bytes);
            segment.write(queue, payload, offset);

            this.current = offset;

            triggerAppend(ledger, 1, offset);
            promise.trySuccess(offset);
        } catch (Throwable t) {
            promise.tryFailure(t);
        } finally {
            payload.release();
        }
    }

    @SuppressWarnings("SameParameterValue")
    private void triggerAppend(int ledger, int limit, Offset offset) {
        if (isNotNull(trigger)) {
            try {
                trigger.onAppend(ledger, limit, offset);
            } catch (Throwable t) {
                if (logger.isWarnEnabled()) {
                    logger.warn("trigger append error: ledger={} limit={} offset={}", ledger, limit, offset);
                }
            }
        }
    }

    private Segment applySegment(int bytes) {
        Segment theTailSegment = tailSegment;
        if (theTailSegment.freeWriteBytes() < bytes) {
            if (segmentLimit >= config.getLogSegmentLimit()) {
                releaseSegment();
            }
            return incrementSegment(StrictMath.max(bytes, config.getLogSegmentSize()));
        } else {
            return theTailSegment;
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
        } else if (limit == 1){
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

    public Segment tailSegment() {
        return tailSegment;
    }
}
