package org.meteor.ledger;

import io.netty.buffer.ByteBuf;
import org.meteor.common.message.Offset;

import java.lang.ref.WeakReference;

/**
 * A cursor for iterating over records in a ledger.
 * The cursor maintains a reference to the current ledger segment
 * and the current position within that segment.
 */
public class LedgerCursor {
    /**
     * Reference to the ledger storage that this cursor is traversing.
     */
    private final LedgerStorage storage;
    /**
     * A {@code WeakReference} to a {@link LedgerSegment} object.
     * This helps manage memory by allowing the {@code LedgerSegment} instance
     * to be garbage-collected when there are no strong references to it.
     */
    private WeakReference<LedgerSegment> segmentWeakReference;
    /**
     * The current position of the cursor within the ledger segment.
     * Used to keep track of the cursor's location for reading or processing ledger entries.
     */
    private int position;

    /**
     * Constructs a new LedgerCursor.
     *
     * @param storage  the storage object containing ledger data.
     * @param segment  the current segment of the ledger.
     * @param position the position within the segment.
     */
    public LedgerCursor(LedgerStorage storage, LedgerSegment segment, int position) {
        this.storage = storage;
        this.segmentWeakReference = new WeakReference<>(segment);
        this.position = position;
    }

    /**
     * Creates a copy of the current LedgerCursor instance.
     *
     * @return a new LedgerCursor instance with the same storage, segment, and position as the original.
     */
    public LedgerCursor copy() {
        return new LedgerCursor(storage, segmentWeakReference.get(), position);
    }

    /**
     * Checks whether there are more elements available in the ledger by
     * verifying the presence of a subsequent ledger segment.
     *
     * @return true if there is another ledger segment available, false otherwise.
     */
    public boolean hashNext() {
        return trundle() != null;
    }

    /**
     * Advances the cursor to the next record and returns it.
     * The cursor reads from the current segment until it finds a record.
     * If no record is found in the current segment, it moves to the next segment
     * and continues searching until a record is found or there are no more segments.
     *
     * @return the next record as a ByteBuf, or null if there are no more records.
     */
    public ByteBuf next() {
        LedgerSegment segment;
        while ((segment = trundle()) != null) {
            ByteBuf record = segment.readRecord(position);
            if (record != null) {
                position += 8 + record.readableBytes();
                return record;
            }
        }
        return null;
    }

    /**
     * Seeks to the specified offset within the ledger. If the given offset is null,
     * it seeks to the tail of the ledger.
     *
     * @param offset the target offset to seek to; if null, seeks to the ledger tail
     * @return this LedgerCursor positioned at the specified offset or the tail if the offset is null
     */
    public LedgerCursor seekTo(Offset offset) {
        if (offset == null) {
            return seekToTail();
        }

        while (true) {
            LedgerSegment present = existingSegment();
            if (!offset.after(present.baseOffset())) {
                return this;
            }

            LedgerSegment next = present.next();
            if (next != null && offset.after(next.baseOffset())) {
                segmentWeakReference = new WeakReference<>(next);
                position = next.basePosition();
                continue;
            }

            int location = present.locate(offset);
            position = Math.max(location, position);
            return this;
        }
    }

    /**
     * Seeks the cursor to the tail of the current ledger segment.
     *
     * @return The current LedgerCursor instance positioned at the tail.
     */
    public LedgerCursor seekToTail() {
        LedgerSegment tail = storage.tailSegment();
        segmentWeakReference = new WeakReference<>(tail);
        position = tail.lastPosition();
        return this;
    }

    /**
     * Traverses through the ledger segments to find the current active segment that contains the
     * position. It continues to move through the segments until it finds an active segment
     * containing the position or returns null if no such segment exists.
     *
     * @return the active LedgerSegment containing the current position or null if no such segment exists
     */
    private LedgerSegment trundle() {
        while (true) {
            LedgerSegment present = existingSegment();
            LedgerSegment next = present.next();

            if (position < present.lastPosition() && present.isActive()) {
                return present;
            }

            if (next == null) {
                return null;
            }

            segmentWeakReference = new WeakReference<>(next);
            position = next.basePosition();
        }
    }

    /**
     * Returns the current position of the LedgerCursor.
     *
     * @return the current position
     */
    public int getPosition() {
        return position;
    }

    /**
     * Retrieves the current active segment, if one exists, from a weak reference.
     * If the segment has been garbage collected, fetches the head segment from storage
     * and updates the weak reference and the position.
     *
     * @return the current active segment, or the head segment if the active segment was garbage collected.
     */
    private LedgerSegment existingSegment() {
        LedgerSegment segment = segmentWeakReference.get();
        if (segment != null) {
            return segment;
        }

        LedgerSegment head = storage.headSegment();
        segmentWeakReference = new WeakReference<>(head);
        position = head.basePosition();
        return head;
    }

    /**
     * Reads the next chunk of data from the current ledger segment up to the specified byte limit.
     *
     * @param bytesLimit The maximum number of bytes to read for the chunk.
     * @return The next ChunkRecord read from the ledger segment, or null if there are no more chunks.
     */
    public ChunkRecord nextChunk(int bytesLimit) {
        LedgerSegment segment;
        while ((segment = trundle()) != null) {
            ChunkRecord record = segment.readChunkRecord(position, bytesLimit);
            if (record != null) {
                position += record.data().readableBytes();
                return record;
            }
        }

        return null;
    }
}
