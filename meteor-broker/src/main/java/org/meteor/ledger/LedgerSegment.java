package org.meteor.ledger;

import io.netty.buffer.ByteBuf;
import org.meteor.common.logging.InternalLogger;
import org.meteor.common.logging.InternalLoggerFactory;
import org.meteor.common.message.Offset;
import org.meteor.remote.util.ByteBufUtil;

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The LedgerSegment class manages a segment of ledger data backed by a ByteBuf.
 * It provides methods to write, read, and locate records within the buffer,
 * along with managing buffer recycling.
 */
public class LedgerSegment {
    /**
     * Logger instance for the LedgerSegment class used for logging messages.
     * This is a static, final variable ensuring it is shared across all instances
     * of LedgerSegment and its value cannot be modified.
     */
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(LedgerSegment.class);
    /**
     * A static reference queue used in the LedgerSegment class to manage the
     * recycling of BufferHolder instances.
     * <p>
     * This queue holds weak references to BufferHolder objects that have been
     * garbage collected, allowing the associated ByteBuf resources to be
     * released properly.
     */
    private static final ReferenceQueue<BufferHolder> BUFFER_RECYCLE_QUEUE = new ReferenceQueue<>();
    /**
     * A static thread initialized to handle buffer recycling processes.
     * This thread continuously monitors the BUFFER_RECYCLE_QUEUE and releases ByteBuf instances
     * that are no longer in use, thus helping to manage the buffer memory efficiently.
     */
    private static final Thread BUFFER_RECYCLE_THREAD;
    /**
     * A map that holds buffers and their corresponding references.
     * <p>
     * This map is used to manage ByteBuf instances using PhantomReferences,
     * ensuring that buffers are properly recycled when they are no longer needed.
     * The keys are the references to the BufferHolder objects, and the values are the ByteBuf instances.
     * The map is thread-safe, leveraging a ConcurrentHashMap for concurrent access.
     */
    private static final Map<Reference<?>, ByteBuf> BUFFERS = new ConcurrentHashMap<>();

    /**
     * The ID of the ledger associated with this segment.
     * This is used to uniquely identify the ledger within the segment.
     */
    private final int ledger;
    /**
     * The baseOffset represents the starting offset for this ledger segment.
     * <p>
     * It is used to mark the position in the ledger from which this segment starts.
     * The offset includes identifying information such as epoch and index.
     * <p>
     * This offset is fundamental in determining the relative positions of records
     * within the ledger segment and is crucial for accurately reading or writing
     * data to the correct locations within the segment.
     */
    private final Offset baseOffset;
    /**
     * Represents the base position within the ledger segment.
     * This is the starting position for records written into this ledger segment.
     */
    private final int basePosition;
    /**
     * The timestamp representing the creation time of the LedgerSegment instance.
     * This value is set when the LedgerSegment is instantiated and remains constant
     * throughout the lifecycle of the object.
     */
    private final long creationTime;
    /**
     * A volatile field that holds a reference to a {@link BufferHolder} instance,
     * enabling safe concurrent access and modifications.
     * <p>
     * This field is used to manage the lifecycle and access of a buffer associated
     * with a ledger segment, providing mechanisms for buffer recycling
     * and proper resource cleanup.
     * <p>
     * The usage of the {@code volatile} keyword ensures visibility of
     * changes to this field across threads, which is critical for
     * maintaining the consistency and integrity of the buffer's lifecycle
     * within the ledger segment's operations.
     */
    private volatile BufferHolder phantomHolder;
    /**
     * Represents the last known offset in the ledger segment.
     * This variable is marked as volatile to ensure visibility among threads.
     */
    private volatile Offset lastOffset;
    /**
     * Represents the last position in the ledger segment.
     * <p>
     * This variable holds the offset of the most recently written position
     * within the current ledger segment. It is used to track the end
     * of the ledger segment for writing and reading operations.
     * <p>
     * The variable is declared as volatile to ensure visibility across threads,
     * preventing thread caching issues in concurrent environments.
     * It is updated as new records are written to the segment.
     */
    private volatile int lastPosition;
    /**
     * Represents the next segment in a ledger's sequence.
     * The volatile keyword ensures that the most up-to-date value is always
     * visible to all threads, providing thread safety in concurrent environments.
     */
    private volatile LedgerSegment next;

    static {
        BUFFER_RECYCLE_THREAD = new Thread(LedgerSegment::releaseDirectBuf, "segment-release");
        BUFFER_RECYCLE_THREAD.setDaemon(true);
        BUFFER_RECYCLE_THREAD.start();
    }

    /**
     * Initializes a new instance of the LedgerSegment class with the specified ledger, buffer, and base offset.
     *
     * @param ledger the ledger identifier.
     * @param buffer the byte buffer to hold data.
     * @param baseOffset the base offset for the segment.
     */
    public LedgerSegment(int ledger, ByteBuf buffer, Offset baseOffset) {
        this.ledger = ledger;
        this.phantomHolder = createBufferHolder(buffer);
        this.lastOffset = this.baseOffset = baseOffset;
        this.lastPosition = this.basePosition = buffer.writerIndex();
        this.creationTime = System.currentTimeMillis();
    }

    /**
     * Continuously attempts to recycle buffers by removing references from the buffer recycle queue,
     * retrieving the corresponding buffers from the buffer map, and releasing them to free resources.
     * This method runs in an infinite loop and handles interruptions by logging errors if enabled.
     * <p>
     * The typical flow of this method is:
     * 1. Continuously poll the BUFFER_RECYCLE_QUEUE for references.
     * 2. For each reference obtained, attempt to remove the associated buffer from the BUFFERS map.
     * 3. If a buffer is found, release it to free its resources.
     * 4. If interrupted during the removal process, log an error if error logging is enabled.
     * <p>
     * Note: This method is designed to run indefinitely as part of a dedicated thread for buffer
     * recycling. Ensure proper shutdown mechanisms are in place to handle the scenario where
     * this thread needs to be stopped gracefully.
     */
    private static void releaseDirectBuf() {
        while (true) {
            try {
                Reference<?> reference = BUFFER_RECYCLE_QUEUE.remove();
                if (reference != null) {
                    ByteBuf buf = BUFFERS.remove(reference);
                    ByteBufUtil.release(buf);
                }
            } catch (InterruptedException e) {
                if (logger.isErrorEnabled()) {
                    logger.error("Release direct buffer failed", e);
                }
            }
        }
    }

    /**
     * Creates a BufferHolder instance using the provided ByteBuf and
     * stores a phantom reference to it in the buffer recycle queue for future reference.
     *
     * @param buf the ByteBuf instance to be held by the BufferHolder
     * @return a BufferHolder containing the provided ByteBuf
     */
    private static BufferHolder createBufferHolder(ByteBuf buf) {
        BufferHolder holder = new BufferHolder(buf);
        BUFFERS.put(new PhantomReference<>(holder, BUFFER_RECYCLE_QUEUE), buf);
        return holder;
    }

    /**
     * Retrieves the creation time of the ledger segment.
     *
     * @return the creation time of the ledger segment.
     */
    long getCreationTime() {
        return creationTime;
    }

    /**
     * Writes a record to the buffer with a given marker, offset, and payload.
     *
     * @param marker an integer marker used to identify the type of record
     * @param offset the Offset object indicating the epoch and index for the record location
     * @param payload the ByteBuf containing the data to be written as the record's payload
     * @throws IllegalStateException if there is an error writing to the buffer or if the segment has been released
     */
    protected void writeRecord(int marker, Offset offset, ByteBuf payload) {
        BufferHolder theHolder = phantomHolder;
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
                throw new IllegalStateException(String.format("Segment write in buffer error - ledger[%d]", ledger), t);
            }

            lastOffset = offset;
            lastPosition = theBuffer.writerIndex();

            return;
        }

        throw new IllegalStateException(String.format("Segment was released - ledger[%d]", ledger));
    }

    /**
     * Reads a record from the buffer at a specified position.
     *
     * @param position the starting position in the buffer to read the record from.
     * @return the record as a ByteBuf, or null if the record could not be read.
     */
    protected ByteBuf readRecord(int position) {
        BufferHolder theHolder = phantomHolder;
        if (theHolder != null) {
            ByteBuf theBuffer = theHolder.buffer;
            int length = theBuffer.getInt(position);
            return theBuffer.retainedSlice(position + 4, length);
        }

        if (logger.isWarnEnabled()) {
            logger.warn("The ledger[{}] record is empty", ledger);
        }
        return null;
    }

    /**
     * Locates the position of the given offset within the ledger segment.
     *
     * @param offset the target offset to locate; if null, returns the last position.
     * @return the position of the given offset within the ledger segment,
     *         or the base position if the offset is before or equal to the base offset,
     *         or the last position if no matching entry is found.
     */
    protected int locate(Offset offset) {
        if (offset == null) {
            return lastPosition;
        } else if (!offset.after(baseOffset)) {
            return basePosition;
        }

        BufferHolder theHolder = phantomHolder;
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

    /**
     * Retrieves the base offset of the ledger segment.
     *
     * @return the base offset of this ledger segment.
     */
    public Offset baseOffset() {
        return baseOffset;
    }

    /**
     * Returns the base position of the ledger segment.
     *
     * @return the base position of the ledger segment
     */
    public int basePosition() {
        return basePosition;
    }

    /**
     * Retrieves the last offset in the ledger segment.
     *
     * @return the last offset recorded in this ledger segment.
     */
    public Offset lastOffset() {
        return lastOffset;
    }

    /**
     * Returns the last position within the ledger segment.
     *
     * @return the last position in the ledger segment
     */
    public int lastPosition() {
        return lastPosition;
    }

    /**
     * Retrieves the next segment in the ledger chain.
     *
     * @return the next LedgerSegment in the sequence, or null if there is no subsequent segment.
     */
    public LedgerSegment next() {
        return next;
    }

    /**
     * Sets the next {@link LedgerSegment} in the sequence.
     *
     * @param next the next {@link LedgerSegment} to be linked
     */
    public void next(LedgerSegment next) {
        this.next = next;
    }

    /**
     * Checks if the current ledger segment is active.
     *
     * @return true if the phantom holder is not null, indicating the segment is active; false otherwise.
     */
    protected boolean isActive() {
        return phantomHolder != null;
    }

    /**
     * Releases resources held by the LedgerSegment instance.
     * <p>
     * This method nullifies the reference to the phantomHolder,
     * freeing up memory and resources associated with it.
     * Intended to be invoked when the LedgerSegment is no longer
     * needed to ensure proper cleanup.
     */
    protected void release() {
        phantomHolder = null;
    }

    /**
     * Calculates the number of writable bytes remaining in the buffer held by the phantomHolder.
     *
     * @return the number of writable bytes if the buffer exists, otherwise 0.
     */
    protected int freeBytes() {
        BufferHolder theHolder = phantomHolder;
        return theHolder == null ? 0 : theHolder.buffer.writableBytes();
    }

    /**
     * Calculates the number of bytes currently used in the buffer held by the phantomHolder.
     *
     * @return the number of readable bytes in the buffer if the phantomHolder is not null; 0 otherwise.
     */
    protected int usedBytes() {
        BufferHolder theHolder = phantomHolder;
        return theHolder == null ? 0 : theHolder.buffer.readableBytes();
    }

    /**
     * Determines the capacity of the buffer held by the current BufferHolder.
     *
     * @return the capacity of the buffer if the BufferHolder is not null;
     *         otherwise, returns 0.
     */
    protected int capacity() {
        BufferHolder theHolder = phantomHolder;
        return theHolder == null ? 0 : theHolder.buffer.capacity();
    }

    /**
     * Writes a chunk of data to the current ledger segment.
     *
     * @param bytes      the number of bytes to be written from the provided buffer
     * @param endOffset  the offset in the ledger where this chunk ends
     * @param buf        the buffer containing the data to be written
     * @throws IllegalStateException if the segment is released or if there is a write error
     */
    public void writeChunkRecord(int bytes, Offset endOffset, ByteBuf buf) {
        final BufferHolder theHolder = phantomHolder;
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
        throw new IllegalStateException("Segment was released");
    }

    /**
     * Reads a chunk record from the buffer starting at the specified position and limited by the specified byte limit.
     *
     * @param position The starting position in the buffer for reading the chunk.
     * @param bytesLimit The maximum number of bytes to read for the chunk.
     * @return A ChunkRecord containing the count of records read and the corresponding ByteBuf data,
     *         or null if the buffer holder is absent.
     */
    public ChunkRecord readChunkRecord(int position, int bytesLimit) {
        BufferHolder theHolder = phantomHolder;
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

    // create BufferHolder instance in heap, alloc buffer memory in direct and both for reduce gc
    private static class BufferHolder {
        ByteBuf buffer;

        public BufferHolder(ByteBuf buffer) {
            this.buffer = buffer;
        }
    }
}
