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

@BenchmarkMode(Mode.All)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 5)
@Threads(1)
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class SegmentBenchmark {
    private static final Map<Reference<?>, ByteBuf> buffers = new ConcurrentHashMap<>();
    private static final ByteBuf message = ByteBufUtil.string2Buf("1");
    private static final Thread BUFFER_RECYCLE_THREAD;
    private static final ReferenceQueue<BufferHolder> BUFFER_RECYCLE_QUEUE = new ReferenceQueue<>();
    private static final BufferHolder theHolder = createBufferHolder(PooledByteBufAllocator.DEFAULT.directBuffer(1024 * 1024, 1024 * 1024));

    static {
        BUFFER_RECYCLE_THREAD = new Thread(SegmentBenchmark::recycleBuffer, "segment-cycle");
        BUFFER_RECYCLE_THREAD.setDaemon(true);
        BUFFER_RECYCLE_THREAD.start();
    }

    private final Offset changeOffset = new Offset(0, 0L);
    private volatile Offset lastOffset = new Offset(0, 0L);
    private volatile int lastPosition = 0;

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

    private static BufferHolder createBufferHolder(ByteBuf buf) {
        BufferHolder holder = new BufferHolder(buf);
        buffers.put(new PhantomReference<>(holder, BUFFER_RECYCLE_QUEUE), buf);
        return holder;
    }

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

    @Benchmark
    public ByteBuf readRecord() {
        ByteBuf theBuffer = theHolder.buffer;
        int length = theBuffer.getInt(0);
        return theBuffer.retainedSlice(4, length);
    }

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
