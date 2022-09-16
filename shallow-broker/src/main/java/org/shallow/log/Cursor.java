package org.shallow.log;

import io.netty.buffer.ByteBuf;
import org.checkerframework.checker.units.qual.C;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.lang.ref.WeakReference;

@ThreadSafe
public class Cursor implements Cloneable {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(LedgerManager.class);

    private final Storage storage;
    private WeakReference<Segment> reference;
    private int location;

    /**
     * Constructs a new object.
     */
    public Cursor(Storage storage, Segment segment, int location) {
        this.storage = storage;
        this.reference = buildWeakReference(segment);
        this.location = location;
    }

    @Override
    public Cursor clone() throws CloneNotSupportedException {
        try {
            return (Cursor)super.clone();
        } catch (Throwable t) {
            throw new CloneNotSupportedException(String.format("Cursor clone failure, location=%d segment=%s", location, reference.get()));
        }
    }

    public ByteBuf next() {
        Segment segment;
        while ((segment = efficientSegment()) != null) {
            ByteBuf buf = segment.read(location);
            if (buf != null) {
                location += 4 + buf.readableBytes();
                return buf;
            }
        }
        return null;
    }

    public boolean hashNext() {
        return efficientSegment() != null;
    }

    @SuppressWarnings("unused")
    public Cursor skip2Location(Offset offset) {
        if (offset == null) {
            return skip2Tail();
        }

       for (;;) {
            Segment segment = currentSegment();
            if (!offset.after(segment.headOffset())) {
                return this;
            }

            Segment next = segment.next();
            if (next != null && offset.after(next.headOffset())) {
                reference = buildWeakReference(next);
                location = next.headLocation();
                continue;
            }

            int position = segment.locate(offset);
            location = StrictMath.max(position, location);
            return this;
        }
    }

    public Cursor skip2Tail() {
        Segment segment = storage.tailSegment();
        reference = buildWeakReference(segment);
        location = segment.tailLocation();

        return this;
    }

    private Segment efficientSegment() {
        for (;;) {
            Segment segment = currentSegment();
            if (location < segment.tailLocation() && segment.isActive()) {
                return segment;
            }

            Segment next = segment.next();
            if (next == null) {
                return null;
            }

            reference = buildWeakReference(next);
            location = next.headLocation();
        }
    }

    private Segment currentSegment() {
        Segment segment = reference.get();
        if (segment != null) {
            return segment;
        }

        Segment headSegment = storage.headSegment();
        reference = buildWeakReference(headSegment);
        location = headSegment.headLocation();
        return headSegment;
    }

    private WeakReference<Segment> buildWeakReference(Segment segment) {
        return new WeakReference<>(segment);
    }
}
