package org.ostara.benchmarks.ledger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.openjdk.jmh.annotations.*;
import org.ostara.common.Offset;
import org.ostara.log.ledger.LedgerConfig;
import org.ostara.log.ledger.LedgerSegment;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.All)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 5)
@Threads(1)
@Fork(1)
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class SegmentBenchmark {

    private LedgerSegmentBenchmarkTest segment;
    private ByteBuf payload;
    private Offset offset;

    @Setup
    public void prepare() {
        payload = allocateBuf();
        offset = new Offset(0, 0L);
        segment = new LedgerSegmentBenchmarkTest(1, payload, offset);
    }

    @Benchmark
    public void testWriteRecord() {
        segment.writeRecord(1, offset, payload);
    }

    @TearDown
    public void clean() {
        payload.release();
    }

    private ByteBuf allocateBuf() {
        LedgerConfig config = new LedgerConfig();
        return PooledByteBufAllocator.DEFAULT.directBuffer(config.segmentBufferCapacity(), config.segmentBufferCapacity());
    }

    static class LedgerSegmentBenchmarkTest extends LedgerSegment {
        public LedgerSegmentBenchmarkTest(int ledger, ByteBuf buffer, Offset baseOffset) {
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
