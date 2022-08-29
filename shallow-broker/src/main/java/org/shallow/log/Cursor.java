package org.shallow.log;

import io.netty.buffer.ByteBuf;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.lang.ref.WeakReference;

import static org.shallow.util.ObjectUtil.isNotNull;
import static org.shallow.util.ObjectUtil.isNull;

@ThreadSafe
public class Cursor {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(LedgerManager.class);

    private final Storage storage;
    private WeakReference<Segment> reference;
    private int location;

    public Cursor(Storage storage, Segment segment, int location) {
        this.storage = storage;
        this.reference = buildWeakReference(segment);
        this.location = location;
    }

    public Cursor copy() {
        return new Cursor(storage, reference.get(), location);
    }

    public ByteBuf next() {
        Segment segment;
        while (isNotNull(segment = efficientSegment())) {
            ByteBuf buf = segment.read(location);
            if (isNotNull(buf)) {
                location += 4 + buf.readableBytes();
                return buf;
            }
        }
        return null;
    }

    public Cursor skip2Location(Offset offset) {
        if (isNull(offset)) {
            return skip2Tail();
        }

        while (true) {
            Segment segment = currentSegment();
            if (!offset.after(segment.headOffset())) {
                return this;
            }

            Segment next = segment.next();
            if (isNotNull(next) && offset.after(next.headOffset())) {
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
        while (true) {
            Segment segment = currentSegment();
            if (location < segment.tailLocation() && segment.isActive()) {
                return segment;
            }

            Segment next = segment.next();
            if (isNull(next)) {
                return null;
            }

            reference = buildWeakReference(next);
            location = next.headLocation();
        }
    }

    private Segment currentSegment() {
        Segment segment = reference.get();
        if (isNotNull(segment)) {
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
