package org.shallow.log;

import io.netty.buffer.ByteBuf;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.netty.util.CharsetUtil.UTF_8;
import static org.shallow.util.ObjectUtil.isNotNull;
import static org.shallow.util.ObjectUtil.isNull;

@ThreadSafe
public class Segment {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(Segment.class);

    private final int ledgerId;

    private final Offset head;
    private final int headLocation;

    private volatile Offset tail;
    private volatile int tailLocation;

    private volatile ByteBufHolder holder;
    private volatile Segment next;

    public Segment(int ledgerId, ByteBuf payload, Offset base) {
        this.ledgerId = ledgerId;
        this.tail = head = base;
        this.holder = constructHolder(payload);
        this.tailLocation = this.headLocation = payload.writerIndex();
    }

    public void write(String queue, ByteBuf payload, Offset offset) {
        ByteBufHolder finalHolder = holder;
        if (isNotNull(finalHolder)) {
            ByteBuf finalBuf = finalHolder.payload;
            int location = finalBuf.writerIndex();

            try {
                int length = queue.length() + 16 + payload.readableBytes();

                finalBuf.writeInt(length);
                finalBuf.writeInt(queue.length());
                finalBuf.writeBytes(queue.getBytes(UTF_8));
                finalBuf.writeInt(offset.epoch());
                finalBuf.writeLong(offset.index());
                finalBuf.writeBytes(payload);

            } catch (Throwable t) {
                finalBuf.writerIndex(location);
                throw new RuntimeException(String.format("Failed to write segment, cause: %s", t));
            }

            tail = offset;
            tailLocation = finalBuf.writerIndex();
            return;
        }
        throw new RuntimeException("Segment is recycled");
    }

    public ByteBuf read(int location) {
        ByteBufHolder finalHolder = holder;
        if (isNotNull(finalHolder)) {
            ByteBuf payload = finalHolder.payload;
            int length = payload.getInt(location);
            return payload.retainedSlice(location + 4, length);
        }

        return null;
    }

    public int freeWriteBytes() {
        ByteBufHolder theHolder = holder;
        return isNull(theHolder) ? 0 : theHolder.payload.writableBytes();
    }

    public void tail(Segment segment) {
        this.next = segment;
    }

    public Segment next() {
        return next;
    }

    public boolean isActive() {
        return isNotNull(holder);
    }

    public Offset tailOffset() {
        return tail;
    }

    public Offset headOffset() {
        return head;
    }

    public int headLocation() {
        return headLocation;
    }

    public int tailLocation() {
        return tailLocation;
    }

    public int getLogId() {
        return ledgerId;
    }

    /**
     * The location of message sites is based on the storage order,
     * <p>offset</p> need to change the offset of the positioning.
     * {@link Segment#write}
     */
    public int locate(Offset offset) {
        if (isNull(offset)) {
            return tailLocation;
        }

        if (!offset.after(head)) {
            return headLocation;
        }

        ByteBufHolder theHolder = holder;
        if (isNull(theHolder)) {
            return tailLocation;
        }

        ByteBuf theBuf = holder.payload;
        int theEpoch = offset.epoch();
        long theIndex = offset.index();

        int limit = tailLocation;
        int position = headLocation;
        while (position < limit) {
            int length = theBuf.getInt(position);
            int queueLength = theBuf.getInt(position + 4);

            int epoch = theBuf.getInt(position + 8 + queueLength);
            if (epoch > theEpoch) {
                return position;
            } else if (epoch == theEpoch){
                long index = theBuf.getLong(position + 8 + queueLength + 4);
                if (index > theIndex) {
                    return position;
                }
            }

            position += length + 4;
        }

        return limit;
    }

    public void release() {
        if (logger.isDebugEnabled()) {
            logger.debug("Release segment of ledger={} headOffset={} tailOffset={}", ledgerId, head, tail);
        }
        this.holder = null;
    }

    private record ByteBufHolder(ByteBuf payload) {
    }

    private static ByteBufHolder constructHolder(ByteBuf payload) {
        ByteBufHolder holder = new ByteBufHolder(payload);
        BYTE_BUF_MAP.put(new PhantomReference<>(holder, BYTE_BUF_HOLDER_REFERENCE_QUEUE), payload);
        return holder;
    }

    private static final ReferenceQueue<ByteBufHolder> BYTE_BUF_HOLDER_REFERENCE_QUEUE = new ReferenceQueue<>();
    private static final Thread BYTE_BUF_HOLDER_THREAD;
    private static final Map<Reference<?>, ByteBuf> BYTE_BUF_MAP = new ConcurrentHashMap<>();

    static {
        Runnable task = () -> {
            while (true) {
                try {
                    Reference<? extends ByteBufHolder> reference = BYTE_BUF_HOLDER_REFERENCE_QUEUE.remove();
                    ByteBuf buf = BYTE_BUF_MAP.remove(reference);

                    if (isNotNull(buf)) {
                        buf.release();
                    }
                } catch (InterruptedException e) {
                    if (logger.isErrorEnabled()) {
                        logger.error(e.getMessage(), e);
                    }
                    break;
                }
            }
        };

        BYTE_BUF_HOLDER_THREAD = new Thread(task, "segment-recycle");
        BYTE_BUF_HOLDER_THREAD.setDaemon(true);
        BYTE_BUF_HOLDER_THREAD.start();
    }
}
