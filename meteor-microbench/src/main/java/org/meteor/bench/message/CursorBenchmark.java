package org.meteor.bench.message;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.meteor.common.message.Offset;
import org.meteor.ledger.*;
import org.meteor.remote.util.NetworkUtil;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.All)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 5)
@Threads(1)
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class CursorBenchmark {
    @Benchmark
    public void testCopy() {
        cursor.copy();
    }

    @Benchmark
    public void testHasNext() {
        cursor.hashNext();
    }

    @Benchmark
    public void testNext() {
        cursor.next();
    }

    @Benchmark
    public void testSeekTo() {
        cursor.seekTo(new Offset(0, 1L));
    }
    @Benchmark
    public void testSeekToTail() {
        cursor.seekToTail();
    }

    @TearDown
    public void clear() {
    }

    private static final LedgerStorage storage = new LedgerStorage(1, "test", 0, new LedgerConfig(),
            NetworkUtil.newEventExecutorGroup(1, "append-record-group").next(), new LedgerTriggerCursorTest());
    private static final ByteBuf buf = allocateBuf();
    private static final LedgerSegmentCursorTest segment = new LedgerSegmentCursorTest(1, buf, new Offset(0, 0L));
    private static final LedgerCursor cursor = new LedgerCursor(storage, segment, 0);

    private static ByteBuf allocateBuf() {
        LedgerConfig config = new LedgerConfig();
        return PooledByteBufAllocator.DEFAULT.directBuffer(config.segmentBufferCapacity(), config.segmentBufferCapacity());
    }

    static class LedgerTriggerCursorTest implements LedgerTrigger {
        @Override
        public void onAppend(int ledgerId, int recordCount, Offset lasetOffset) {
        }

        @Override
        public void onRelease(int ledgerId, Offset oldHeadOffset, Offset newHeadOffset) {
            // do nothing
        }
    }

    static class LedgerSegmentCursorTest extends LedgerSegment {
        public LedgerSegmentCursorTest(int ledger, ByteBuf buffer, Offset baseOffset) {
            super(ledger, buffer, baseOffset);
        }

        @Override
        protected void writeRecord(int marker, Offset offset, ByteBuf payload) {
            super.writeRecord(marker, offset, payload);
        }

        @Override
        protected ByteBuf readRecord(int position) {
            return super.readRecord(position);
        }

        @Override
        protected int locate(Offset offset) {
            return super.locate(offset);
        }

        @Override
        public Offset baseOffset() {
            return super.baseOffset();
        }

        @Override
        public int basePosition() {
            return super.basePosition();
        }

        @Override
        public Offset lastOffset() {
            return super.lastOffset();
        }

        @Override
        public int lastPosition() {
            return super.lastPosition();
        }

        @Override
        public LedgerSegment next() {
            return super.next();
        }

        @Override
        public void next(LedgerSegment next) {
            super.next(next);
        }

        @Override
        protected boolean isActive() {
            return super.isActive();
        }

        @Override
        protected void release() {
            super.release();
        }

        @Override
        protected int freeBytes() {
            return super.freeBytes();
        }

        @Override
        protected int usedBytes() {
            return super.usedBytes();
        }

        @Override
        protected int capacity() {
            return super.capacity();
        }
    }
}
