package org.ephemq.bench.message;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.ephemq.common.message.Offset;
import org.ephemq.ledger.*;
import org.ephemq.remote.util.NetworkUtil;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

/**
 * Benchmark suite for various cursor operations in a ledger system using JMH.
 * This class is designed to measure the performance of basic cursor operations
 * such as copying, seeking, and retrieving next elements.
 */
@BenchmarkMode(Mode.All)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 5)
@Threads(1)
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class CursorBenchmark {
    /**
     * A statically initialized instance of LedgerStorage used in the CursorBenchmark class.
     * This storage instance is configured with specific parameters including an ID of 1,
     * a name of "test", an index of 0, an instance of LedgerConfig, an executor group
     * created by NetworkUtil, and a LedgerTriggerCursorTest instance.
     */
    private static final LedgerStorage storage = new LedgerStorage(1, "test", 0, new LedgerConfig(),
            NetworkUtil.newEventExecutorGroup(1, "append-record-group").next(), new LedgerTriggerCursorTest());
    /**
     * A statically allocated ByteBuf used for buffering operations
     * within the CursorBenchmark class. This buffer is pre-allocated
     * based on configuration settings defined in the LedgerConfig.
     */
    private static final ByteBuf buf = allocateBuf();
    /**
     * A cursor for representing a test segment in the ledger.
     * This segment is used for benchmarking cursor operations.
     */
    private static final LedgerSegmentCursorTest segment = new LedgerSegmentCursorTest(1, buf, new Offset(0, 0L));
    /**
     * A static final instance of the LedgerCursor used for benchmarking purposes
     * in the CursorBenchmark class. This specific cursor is initialized with
     * references to the storage, segment, and a starting position of 0.
     */
    private static final LedgerCursor cursor = new LedgerCursor(storage, segment, 0);

    /**
     * Allocates a direct ByteBuf using the default PooledByteBufAllocator.
     * The capacity of the buffer is determined by the segmentBufferCapacity
     * defined in the LedgerConfig.
     *
     * @return a direct ByteBuf with initial and maximum capacities set
     * to the segmentBufferCapacity from the LedgerConfig.
     */
    private static ByteBuf allocateBuf() {
        LedgerConfig config = new LedgerConfig();
        return PooledByteBufAllocator.DEFAULT.directBuffer(config.segmentBufferCapacity(), config.segmentBufferCapacity());
    }

    /**
     * Benchmarks the performance of the copy() operation on the LedgerCursor.
     * This method is designed to be used with JMH to measure throughput,
     * average time taken, sample time, or single-shot time for the cursor's copy operation.
     * <p>
     * The cursor.copy() method creates a new LedgerCursor instance with the same storage,
     * segment, and position as the original cursor, effectively duplicating its state.
     * <p>
     * The benchmark involves invoking the copy operation in various scenarios
     * to measure its performance characteristics under different conditions.
     */
    @Benchmark
    public void testCopy() {
        cursor.copy();
    }

    /**
     * Implements a benchmark test to measure the performance of the `hashNext` method.
     * This method checks whether there are more elements available in the ledger.
     * It is part of a suite of performance tests intended to evaluate different operations' efficiency.
     * <p>
     * The method is annotated with {@link Benchmark} to signify that it is a performance benchmark test.
     * This method will be executed repeatedly, and the performance metrics will be recorded.
     * <p>
     * Note that this method should only be invoked within a benchmarking context, typically managed
     * by a performance testing framework like JMH (Java Microbenchmark Harness).
     * <p>
     * The performance metrics obtained from this method can help in identifying bottlenecks and
     * optimizing the `hashNext` implementation.
     */
    @Benchmark
    public void testHasNext() {
        cursor.hashNext();
    }

    /**
     * Benchmarks the performance of advancing the cursor to the next record.
     * This method invokes the `next` method on the cursor, which attempts to
     * read the next record from the current segment, moving to the next segment
     * if no record is found, until a record is found or there are no more segments.
     */
    @Benchmark
    public void testNext() {
        cursor.next();
    }

    /**
     * Benchmark method for testing the performance of the `seekTo` operation
     * on a LedgerCursor. This method uses an Offset object with an epoch of 0
     * and an index of 1L to seek to a specific position within the ledger.
     */
    @Benchmark
    public void testSeekTo() {
        cursor.seekTo(new Offset(0, 1L));
    }

    /**
     * Benchmarks the performance of seeking the cursor to the tail of the current ledger segment.
     * This method is part of the CursorBenchmark suite and is intended to be used with a benchmarking
     * framework to measure throughput and performance characteristics.
     * <p>
     * This method utilizes the {@code seekToTail} functionality of the cursor to position it
     * at the last record of the ledger and is critical for evaluating scenarios where rapid
     * forward traversal of ledger segments is necessary.
     * <p>
     * Implementing classes and frameworks should ensure that the environment is properly set up
     * and conditions are controlled to obtain accurate measurement results.
     */
    @Benchmark
    public void testSeekToTail() {
        cursor.seekToTail();
    }

    /**
     * Cleans up resources after a benchmark run.
     * This method is called to release any resources allocated during the benchmarks,
     * ensuring that repeated runs do not interfere with one another.
     */
    @TearDown
    public void clear() {
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
        /**
         * Constructs a new LedgerSegmentCursorTest instance with the specified ledger, byte buffer, and base offset.
         *
         * @param ledger     the ledger identifier.
         * @param buffer     the byte buffer to hold data.
         * @param baseOffset the base offset for the segment.
         */
        public LedgerSegmentCursorTest(int ledger, ByteBuf buffer, Offset baseOffset) {
            super(ledger, buffer, baseOffset);
        }

        /**
         * Writes a record to the buffer with the specified marker, offset, and payload.
         *
         * @param marker  an integer marker used to identify the type of record
         * @param offset  the {@code Offset} object indicating the epoch and index for the record location
         * @param payload the {@code ByteBuf} containing the data to be written as the record's payload
         * @throws IllegalStateException if there is an error writing to the buffer or if the segment has been released
         */
        @Override
        protected void writeRecord(int marker, Offset offset, ByteBuf payload) {
            super.writeRecord(marker, offset, payload);
        }

        /**
         * Reads a record from the buffer at a specified position.
         *
         * @param position the starting position in the buffer to read the record from.
         * @return the record as a ByteBuf, or null if the record could not be read.
         */
        @Override
        protected ByteBuf readRecord(int position) {
            return super.readRecord(position);
        }

        /**
         * Locates the position of the given offset within the ledger segment.
         *
         * @param offset the target offset to locate; if null, returns the last position.
         * @return the position of the given offset within the ledger segment,
         *         or the base position if the offset is before or equal to the base offset,
         *         or the last position if no matching entry is found.
         */
        @Override
        protected int locate(Offset offset) {
            return super.locate(offset);
        }

        /**
         * Retrieves the base offset of the ledger segment.
         *
         * @return the base offset of this ledger segment.
         */
        @Override
        public Offset baseOffset() {
            return super.baseOffset();
        }

        /**
         * Returns the base position of the ledger segment.
         *
         * @return the base position of the ledger segment
         */
        @Override
        public int basePosition() {
            return super.basePosition();
        }

        /**
         * Retrieves the last offset in the ledger segment.
         *
         * @return the last offset recorded in this ledger segment.
         */
        @Override
        public Offset lastOffset() {
            return super.lastOffset();
        }

        /**
         * Returns the last position within the ledger segment.
         *
         * @return the last position in the ledger segment
         */
        @Override
        public int lastPosition() {
            return super.lastPosition();
        }

        /**
         * Retrieves the next LedgerSegment in the sequence.
         *
         * @return the next LedgerSegment
         */
        @Override
        public LedgerSegment next() {
            return super.next();
        }

        /**
         * Sets the next LedgerSegment in the sequence.
         *
         * @param next the LedgerSegment to be set as next
         */
        @Override
        public void next(LedgerSegment next) {
            super.next(next);
        }

        /**
         * Checks if the current ledger segment within the context of the LedgerSegmentCursorTest is active.
         *
         * @return true if the phantom holder is not null, indicating the segment is active; false otherwise.
         */
        @Override
        protected boolean isActive() {
            return super.isActive();
        }

        /**
         * Releases resources held by this LedgerSegmentCursorTest instance.
         * <p>
         * This method is an override that calls the superclass's release method,
         * ensuring that any resources managed by the superclass are properly
         * released. Intended to be invoked when the LedgerSegmentCursorTest
         * instance is no longer needed to ensure proper cleanup.
         */
        @Override
        protected void release() {
            super.release();
        }

        /**
         * Calculates the number of writable bytes remaining in the buffer held by the phantomHolder.
         *
         * @return the number of writable bytes if the buffer exists, otherwise 0.
         */
        @Override
        protected int freeBytes() {
            return super.freeBytes();
        }

        /**
         * Calculates the number of bytes currently used in the buffer.
         *
         * @return the number of readable bytes in the buffer if available; 0 otherwise.
         */
        @Override
        protected int usedBytes() {
            return super.usedBytes();
        }

        /**
         * Determines the capacity of the buffer held by the current BufferHolder.
         *
         * @return the capacity of the buffer if the BufferHolder is not null;
         *         otherwise, returns 0.
         */
        @Override
        protected int capacity() {
            return super.capacity();
        }
    }
}
