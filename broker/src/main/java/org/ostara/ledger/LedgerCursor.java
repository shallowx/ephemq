package org.ostara.ledger;

import io.netty.buffer.ByteBuf;
import org.ostara.common.Offset;

import java.lang.ref.WeakReference;

public class LedgerCursor {
    private final LedgerStorage storage;
    private WeakReference<LedgerSegment> segmentHolder;
    private int position;

    public LedgerCursor(LedgerStorage storage, LedgerSegment segment, int position) {
        this.storage = storage;
        this.segmentHolder = new WeakReference<>(segment);
        this.position = position;
    }

    public LedgerCursor copy() {
        return new LedgerCursor(storage, segmentHolder.get(), position);
    }

    public boolean hashNext() {
        return rollValidSegment() != null;
    }

    public ByteBuf next() {
        LedgerSegment segment;
        while ((segment = rollValidSegment()) != null) {
            ByteBuf record = segment.readRecord(position);
            if (record != null) {
                position += 8 + record.readableBytes();
                return record;
            }
        }
        return null;
    }

    public LedgerCursor seekTo(Offset offset) {
        if (offset == null) {
            return seekToTail();
        }

        while (true) {
            LedgerSegment present = presentSegment();
            if (!offset.after(present.baseOffset())) {
                return this;
            }

            LedgerSegment next = present.next();
            if (next != null && offset.after(next.baseOffset())) {
                segmentHolder = new WeakReference<>(next);
                position = next.basePosition();
                continue;
            }

            int location = present.locate(offset);
            position = Math.max(location, position);
            return this;
        }
    }

    public LedgerCursor seekToTail() {
        LedgerSegment tail = storage.tailSegment();
        segmentHolder = new WeakReference<>(tail);
        position = tail.lastPosition();
        return this;
    }

    private LedgerSegment rollValidSegment() {
        while (true) {
            LedgerSegment present = presentSegment();
            LedgerSegment next = present.next();

            if (position < present.lastPosition() && present.isActive()) {
                return present;
            }

            if (next == null) {
                return null;
            }

            segmentHolder = new WeakReference<>(next);
            position = next.basePosition();
        }
    }

    public int getPosition() {
        return position;
    }

    private LedgerSegment presentSegment() {
        LedgerSegment segment = segmentHolder.get();
        if (segment != null) {
            return segment;
        }

        LedgerSegment head = storage.headSegment();
        segmentHolder = new WeakReference<>(head);
        position = head.basePosition();
        return head;
    }
}
