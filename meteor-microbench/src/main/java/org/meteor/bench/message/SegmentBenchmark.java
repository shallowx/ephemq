package org.meteor.bench.message;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.meteor.common.message.Offset;
import org.meteor.remote.util.ByteBufUtil;
import org.openjdk.jmh.annotations.*;

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * The SegmentBenchmark class contains methods to benchmark the performance of writing and reading segment records using ByteBuf.
 * It uses the JMH (Java Microbenchmark Harness) framework to measure the performance. The class includes benchmarks for writing
 * records, reading records, writing chunk records, and reading chunk records.
 * <p>
 * The class uses a static collection to manage buffers and includes reference queues to aid in garbage collection of unused buffers.
 * A separate thread is used to recycle buffers when they are no longer needed.
 * <p>
 * An inner BufferHolder class is used to hold instances of ByteBuf, which are used during the benchmarking operations.
 * <p>
 * The @BenchmarkMode, @Warmup, @Measurement, @Threads, @State, and @OutputTimeUnit annotations from JMH are applied to configure
 * the benchmark settings, including the mode, warmup iterations, measurement iterations, thread count, state scope, and output time unit.
 * <p>
 * Methods:
 * - writeRecord(): Benchmarks the performance of writing a record to a ByteBuf.
 * - readRecord(): Benchmarks the performance of reading a record from a ByteBuf.
 * - writeChunkRecord(): Benchmarks the performance of writing a chunk record to a ByteBuf.
 * - readChunkRecord(): Benchmarks the performance of reading a chunk record from a ByteBuf.
 */
@BenchmarkMode(Mode.All)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 5)
@Threads(1)
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class SegmentBenchmark {
    /**
     * A thread-safe map that associates a ByteBuf with a reference, enabling
     * buffer recycling within the application. Managed using a ConcurrentHashMap
     * to ensure safe concurrent access. This map aids in efficiently handling
     * buffers by storing and allowing for their recycling.
     */
    private static final Map<Reference<?>, ByteBuf> buffers = new ConcurrentHashMap<>();
    /**
     * A ByteBuf instance initialized with the string "1".
     * This buffer is immutable and is used in the SegmentBenchmark class
     * for performing various operations in its benchmarks.
     */
    private static final ByteBuf message = ByteBufUtil.string2Buf("1");
    /**
     * Dedicated thread for recycling ByteBuf instances to manage memory efficiently
     * and reduce garbage collection overhead. This thread continuously attempts
     * to reclaim and recycle ByteBuf instances that are no longer in use by monitoring
     * a reference queue (BUFFER_RECYCLE_QUEUE).
     */
    private static final Thread BUFFER_RECYCLE_THREAD;
    /**
     * A {@code ReferenceQueue} used to manage {@code BufferHolder} objects, enabling efficient
     * cleanup and recycling of buffers when they are no longer in use.
     *
     * This queue is monitored by a dedicated thread that processes references enqueued
     * into the {@code BUFFER_RECYCLE_QUEUE}. When a {@code BufferHolder} is enqueued
     * to this queue, it indicates that the associated buffer is ready for recycling,
     * and the corresponding {@code ByteBuf} will be released to free up resources.
     *
     * The {@code BUFFER_RECYCLE_QUEUE} works in conjunction with phantom references
     * to track the lifecycle of {@code BufferHolder} objects. When the JVM detects
     * that a {@code BufferHolder} is only softly reachable, the phantom reference
     * is cleared and added to this queue.
     *
     * This mechanism is crucial for managing buffer lifecycles and freeing up memory
     * resources in a timely manner, preventing potential memory leaks and ensuring
     * optimal performance in high-throughput scenarios.
     */
    private static final ReferenceQueue<BufferHolder> BUFFER_RECYCLE_QUEUE = new ReferenceQueue<>();
    /**
     * A static constant BufferHolder instance used to hold a direct ByteBuf of size 1MB.
     * This buffer is allocated from a PooledByteBufAllocator for efficient memory management.
     */
    private static final BufferHolder theHolder = createBufferHolder(PooledByteBufAllocator.DEFAULT.directBuffer(1024 * 1024, 1024 * 1024));

    static {
        BUFFER_RECYCLE_THREAD = new Thread(SegmentBenchmark::recycleBuffer, "segment-cycle");
        BUFFER_RECYCLE_THREAD.setDaemon(true);
        BUFFER_RECYCLE_THREAD.start();
    }

    /**
     * Represents the initial offset used for tracking changes in the segment stream during benchmarking.
     * It's a constant value initialized with epoch 0 and index 0.
     */
    private final Offset changeOffset = new Offset(0, 0L);
    /**
     * The last observed offset in the stream, storing the most recent epoch and index.
     * This variable is volatile to ensure visibility between threads.
     */
    private volatile Offset lastOffset = new Offset(0, 0L);
    /**
     * The last known position in the byte buffer stream being processed.
     * This variable is volatile to ensure visibility across multiple threads.
     */
    private volatile int lastPosition = 0;

    /**
     * Recycles buffer objects that are no longer in use.
     *
     * Continuously removes references from the BUFFER_RECYCLE_QUEUE,
     * retrieves the corresponding ByteBuf from the buffers map,
     * and releases the ByteBuf if it is not null.
     *
     * Terminates the loop if InterruptedException is caught.
     */
    private static void recycleBuffer() {
        while (true) {
            try {
                Reference<?> reference = BUFFER_RECYCLE_QUEUE.remove();
                ByteBuf buf = buffers.remove(reference);
                if (buf != null) {
                    buf.release();
                }
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    /**
     * Creates a BufferHolder containing the specified ByteBuf and adds it to a recycling queue for later release.
     *
     * @param buf the ByteBuf to be held by the new BufferHolder
     * @return a new BufferHolder containing the specified ByteBuf
     */
    private static BufferHolder createBufferHolder(ByteBuf buf) {
        BufferHolder holder = new BufferHolder(buf);
        buffers.put(new PhantomReference<>(holder, BUFFER_RECYCLE_QUEUE), buf);
        return holder;
    }

    /**
     * A benchmarking method that writes a record to a ByteBuf.
     *
     * In this method, the ByteBuf's writerIndex is saved prior to attempting
     * to write a record. If an exception is thrown during the write attempt,
     * the writerIndex is reset to its original value and an IllegalStateException
     * is thrown with information about the location of the error.
     *
     * The method also updates the lastOffset and lastPosition fields with the
     * current values of changeOffset and the ByteBuf writerIndex, respectively.
     *
     * Note: The body of the try block is intentionally left empty as it
     * pertains to a Netty unit test, which is outside the scope of this
     * performance benchmarking.
     *
     * @throws IllegalStateException if any error occurs during the segment write operation
     */
    @Benchmark
    public void writeRecord() {
        ByteBuf theBuffer = theHolder.buffer;
        int location = theBuffer.writerIndex();
        try {
            // keep empty, because this is about netty unittest, and not need included in the scope of performance testing
        } catch (Throwable t) {
            theBuffer.writerIndex(location);
            throw new IllegalStateException(String.format("Segment write error, location:%d", location), t);
        }

        lastOffset = changeOffset;
        lastPosition = theBuffer.writerIndex();
    }

    /**
     * Reads a record from the buffer held by the current instance of BufferHolder.
     *
     * @return a retained slice of the buffer starting from the 4th byte to the length specified in the first 4 bytes.
     */
    @Benchmark
    public ByteBuf readRecord() {
        ByteBuf theBuffer = theHolder.buffer;
        int length = theBuffer.getInt(0);
        return theBuffer.retainedSlice(4, length);
    }

    /**
     * Writes a chunk record to the buffer and manages the buffer's writer index.
     * This method is used for benchmarking purposes and includes necessary error handling to reset
     * the buffer's writer index in case of an exception. The implementation details related to actual
     * writing are intentionally omitted as they are outside the scope of the performance testing.
     *
     * Benchmarked under various controls to measure throughput, average time, sample time, and single-shot time.
     * In case of failure, it resets the buffer's writer index to its original location and throws an exception.
     *
     * @throws IllegalStateException if there is an error during writing the segment
     */
    @Benchmark
    public void writeChunkRecord() {
        ByteBuf theBuffer = theHolder.buffer;
        int location = theBuffer.writerIndex();
        try {
            // keep empty
            // this is netty part not included in the meteor dev scope of performance testing
        } catch (Throwable t) {
            theBuffer.writerIndex(location);
            throw new IllegalStateException("Segment write error", t);
        }
    }

    /**
     * Reads and processes a chunk of records from a buffer until a certain byte limit or
     * record count is reached.
     *
     * This method uses the position set by the class field `lastPosition` to determine
     * the limit for reading. It reads an integer length from the buffer at each position
     * and calculates the total bytes to be processed, including an additional 8 bytes.
     * The process stops when the total bytes exceed 100 and at least one record has been read.
     *
     * Fields used:
     * - `lastPosition`: The upper limit for reading from the buffer.
     * - `theHolder`: Holds the buffer from which the records are read.
     */
    @Benchmark
    public void readChunkRecord() {
        int limit = lastPosition;
        int count = 0;
        int bytes = 0;
        int location = 0;
        while (location < limit) {
            int length = theHolder.buffer.getInt(location);
            int theBytes = 8 + length;
            if (theBytes + bytes > 100 && count != 0) {
                break;
            }
            count++;
            bytes += theBytes;
            location += theBytes;
        }
    }

    private static class BufferHolder {
        ByteBuf buffer;

        public BufferHolder(ByteBuf buffer) {
            this.buffer = buffer;
        }
    }
}
