package org.meteor.ledger;

import io.netty.buffer.ByteBuf;
import org.meteor.common.Offset;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LedgerSegment {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(LedgerSegment.class);
    private static final ReferenceQueue<BufferHolder> BUFFER_RECYCLE_QUEUE = new ReferenceQueue<>();
    private static final Thread BUFFER_RECYCLE_THREAD;
    private static final Map<Reference<?>, ByteBuf> buffers = new ConcurrentHashMap<>();

    static {
        BUFFER_RECYCLE_THREAD = new Thread(LedgerSegment::recycleBuffer, "segment-cycle");
        BUFFER_RECYCLE_THREAD.setDaemon(true);
        BUFFER_RECYCLE_THREAD.start();
    }

    private final int ledger;
    private final Offset baseOffset;
    private final int basePosition;
    private final long creationTime;
    private volatile BufferHolder holder;
    private volatile Offset lastOffset;
    private volatile int lastPosition;
    private volatile LedgerSegment next;

    public LedgerSegment(int ledger, ByteBuf buffer, Offset baseOffset) {
        this.ledger = ledger;
        this.holder = createBufferHolder(buffer);
        this.lastOffset = this.baseOffset = baseOffset;
        this.lastPosition = this.basePosition = buffer.writerIndex();
        this.creationTime = System.currentTimeMillis();
    }

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

    long getCreationTime() {
        return creationTime;
    }

    protected void writeRecord(int marker, Offset offset, ByteBuf payload) {
        BufferHolder theHolder = holder;
        if (theHolder != null) {
            ByteBuf theBuffer = theHolder.buffer;
            int location = theBuffer.writerIndex();
            try {
                int length = 16 + payload.readableBytes();

                theBuffer.writeInt(length);
                theBuffer.writeInt(marker);
                theBuffer.writeInt(offset.getEpoch());
                theBuffer.writeLong(offset.getIndex());
                theBuffer.writeBytes(payload);
                theBuffer.writeInt(length);
            } catch (Throwable t) {
                theBuffer.writerIndex(location);
                throw new IllegalStateException(String.format("Segment write error, ledger=%s", ledger), t);
            }

            lastOffset = offset;
            lastPosition = theBuffer.writerIndex();

            return;
        }

        throw new IllegalStateException(String.format("Segment was released, ledger=%s", ledger));
    }

    protected ByteBuf readRecord(int position) {
        BufferHolder theHolder = holder;
        if (theHolder != null) {
            ByteBuf theBuffer = theHolder.buffer;
            int length = theBuffer.getInt(position);
            return theBuffer.retainedSlice(position + 4, length);
        }
        logger.warn("The record is empty, and ledger={}", ledger);
        return null;
    }

    protected int locate(Offset offset) {
        if (offset == null) {
            return lastPosition;
        } else if (!offset.after(baseOffset)) {
            return basePosition;
        }

        BufferHolder theHolder = holder;
        if (theHolder == null) {
            return lastPosition;
        }

        int theEpoch = offset.getEpoch();
        long theIndex = offset.getIndex();
        int limit = lastPosition;

        int location = basePosition;
        ByteBuf theBuffer = theHolder.buffer;
        while (location < limit) {
            int length = theBuffer.getInt(location);
            int epoch = theBuffer.getInt(location + 8);
            if (epoch > theEpoch) {
                return location;
            } else if (epoch == theEpoch) {
                long index = theBuffer.getLong(location + 12);
                if (index > theIndex) {
                    return location;
                }
            }
            location += 8 + length;
        }
        return limit;
    }

    public Offset baseOffset() {
        return baseOffset;
    }

    public int basePosition() {
        return basePosition;
    }

    public Offset lastOffset() {
        return lastOffset;
    }

    public int lastPosition() {
        return lastPosition;
    }

    public LedgerSegment next() {
        return next;
    }

    public void next(LedgerSegment next) {
        this.next = next;
    }

    protected boolean isActive() {
        return holder != null;
    }

    protected void release() {
        holder = null;
    }

    protected int freeBytes() {
        BufferHolder theHolder = holder;
        return theHolder == null ? 0 : theHolder.buffer.writableBytes();
    }

    protected int usedBytes() {
        BufferHolder theHolder = holder;
        return theHolder == null ? 0 : theHolder.buffer.readableBytes();
    }

    protected int capacity() {
        BufferHolder theHolder = holder;
        return theHolder == null ? 0 : theHolder.buffer.capacity();
    }

    public void writeChunkRecord(int bytes, Offset endOffset, ByteBuf buf) {
        final BufferHolder theHolder = holder;
        if (theHolder != null) {
            ByteBuf theBuffer = theHolder.buffer;
            int location = theBuffer.writerIndex();
            try {
                theBuffer.writeBytes(buf, bytes);
            } catch (Throwable t) {
                theBuffer.writerIndex(location);
                throw new IllegalStateException("Segment write error", t);
            }
            lastOffset = endOffset;
            lastPosition = theBuffer.writerIndex();
            return;
        }
        throw new IllegalStateException("segment is released");
    }

    public ChunkRecord readChunkRecord(int position, int bytesLimit) {
        BufferHolder theHolder = holder;
        if (theHolder != null) {
            ByteBuf theBuffer = theHolder.buffer;
            int limit = lastPosition;
            int count = 0;
            int bytes = 0;
            int location = position;
            while (location < limit) {
                int length = theBuffer.getInt(location);
                int theBytes = 8 + length;
                if (theBytes + bytes > bytesLimit && count != 0) {
                    break;
                }
                count++;
                bytes += theBytes;
                location += theBytes;
            }

            ByteBuf buf = theBuffer.retainedSlice(position, bytes);
            return new ChunkRecord(count, buf);
        }
        return null;
    }

    private static class BufferHolder {
        ByteBuf buffer;

        public BufferHolder(ByteBuf buffer) {
            this.buffer = buffer;
        }
    }
}
