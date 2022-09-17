package org.shallow.log;

import io.netty.buffer.*;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Promise;
import org.shallow.RemoteException;
import org.shallow.consumer.pull.PullResult;
import org.shallow.internal.config.BrokerConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.util.ByteBufUtil;
import javax.annotation.concurrent.ThreadSafe;

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

    /**
     * Constructs a new object.
     */
    public Storage(EventExecutor storageExecutor, int ledger, BrokerConfig config, int epoch, LedgerTrigger trigger) {
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
            Offset offset = new Offset(theCurrent.epoch(), theCurrent.index() + 1);

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

    public void read(int requestId, String topic, String queue, short version, Offset offset, int limit, Promise<PullResult> promise) {
        if (storageExecutor.inEventLoop()) {
            doRead(requestId, topic, queue, version, offset, limit, promise);
        } else {
            storageExecutor.execute(() -> doRead(requestId, topic, queue, version, offset,  limit, promise));
        }
    }

    @SuppressWarnings("all")
    private void doRead(int requestId, String topic, String queue, short version, Offset offset, int limit, Promise<PullResult> promise) {
        try {
            Segment segment = locateSegment(offset);
            if (segment == null) {
                promise.tryFailure(RemoteException.of(RemoteException.Failure.SUBSCRIBE_EXCEPTION, String.format("The Segment not found, and the offset<epoch= %d index=%d>", offset.epoch(), offset.index())));
                return;
            }

            int position = segment.locate(offset);
            CompositeByteBuf compositeByteBuf = null;
            int current = 0;

            int limitBytes = config.getMessagePullBytesLimit();
            while (current < limit) {
                ByteBuf queueBuf = null;
                ByteBuf topicBuf = null;
                try {
                    int tailLocation = segment.tailLocation();
                    if (position >= tailLocation) {
                        Segment next = segment.next();
                        if (next == null) {
                            break;
                        }
                        segment = next;
                    }

                    ByteBuf payload = segment.readCompleted(position);

                    short theVersion = payload.getShort(4);

                    int topicLength = payload.getInt(6);
                    topicBuf = payload.retainedSlice(10, topicLength);
                    String theTopic = ByteBufUtil.buf2String(topicBuf, topicLength);

                    int queueLength = payload.getInt(10 + topicLength);
                    queueBuf = payload.retainedSlice(14 + topicLength, queueLength);
                    String theQueue = ByteBufUtil.buf2String(queueBuf, queueLength);

                    position += payload.readableBytes();

                    if (!theTopic.equals(topic)) {
                        continue;
                    }

                    if (!theQueue.equals(queue)) {
                        continue;
                    }

                    if (version != -1 && theVersion != version) {
                        continue;
                    }

                    if (compositeByteBuf == null) {
                        if (payload.readableBytes() > limitBytes) {
                            break;
                        }

                        compositeByteBuf = newComposite(payload, limit);
                        continue;
                    }

                    int readableBytes = compositeByteBuf.readableBytes() + payload.readableBytes();
                    if (readableBytes > limitBytes) {
                        break;
                    }

                    compositeByteBuf.addFlattenedComponents(true, payload);
                    current++;
                } catch (Throwable t) {
                    if (logger.isErrorEnabled()) {
                        logger.error("Read message failed, error:{}", t);
                    }
                    continue;
                } finally {
                    ByteBufUtil.release(queueBuf);
                }
            }

            triggerPull(requestId, queue, version, ledger, limit, offset, compositeByteBuf == null ? Unpooled.EMPTY_BUFFER : compositeByteBuf);

            Offset tailOffset = segment.tailOffset();
            promise.trySuccess(new PullResult(ledger, null, queue, limit, tailOffset.epoch(), tailOffset.index(), null));
        } catch (Throwable t) {
            promise.tryFailure(t);
        }
    }

    private CompositeByteBuf newComposite(ByteBuf buf, int limit) {
        return Unpooled.compositeBuffer(limit).addFlattenedComponents(true, buf);
    }

    @SuppressWarnings("SameParameterValue")
    private void triggerPull(int requestId, String queue, short version, int ledger, int limit, Offset offset, ByteBuf buf) {
        if (trigger != null) {
            try {
                trigger.onPull(requestId, queue, version, ledger, limit, offset, buf);
            } catch (Throwable t) {
                if (logger.isWarnEnabled()) {
                    logger.warn("trigger pull error: ledger={} limit={} offset={}. error:{}", ledger, limit, offset, t);
                }
            }
        }
    }

    @SuppressWarnings("SameParameterValue")
    private void triggerAppend(int ledger, int limit, Offset offset) {
        if (trigger != null) {
            try {
                trigger.onAppend(limit, offset);
            } catch (Throwable t) {
                if (logger.isWarnEnabled()) {
                    logger.warn("trigger append error: ledger={} limit={} offset={}", ledger, limit, offset);
                }
            }
        }
    }

    public Cursor locateCursor(Offset offset) {
        if (offset == null) {
            Segment theTailSegment = tailSegment;
            return new Cursor(this, theTailSegment, theTailSegment.tailLocation());
        }
        Segment theHeadSegment = headSegment;
        return new Cursor(this, theHeadSegment, theHeadSegment.headLocation());
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
            if (logger.isErrorEnabled()) {
                logger.error("Failed to locate segment, error:{}", t);
            }
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
