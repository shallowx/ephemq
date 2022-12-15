package org.ostara.ledger;

import static io.netty.util.CharsetUtil.UTF_8;
import io.netty.buffer.ByteBuf;
import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.concurrent.ThreadSafe;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;

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

    public void write(String topic, String queue, short version, ByteBuf payload, Offset offset) {
        ByteBufHolder finalHolder = holder;
        if (finalHolder != null) {
            ByteBuf finalBuf = finalHolder.payload;
            int location = finalBuf.writerIndex();

            try {
                int topicLength = topic.length();
                int queueLength = queue.length();

                int length = topicLength + queueLength + 22 + payload.readableBytes();

                finalBuf.writeInt(length);
                finalBuf.writeShort(version);

                finalBuf.writeInt(topicLength);
                finalBuf.writeBytes(topic.getBytes(UTF_8));

                finalBuf.writeInt(queueLength);
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

        throw new RuntimeException("Segment has been recycled");
    }

    public ByteBuf read(int location) {
        ByteBufHolder finalHolder = holder;
        if (finalHolder != null) {
            ByteBuf payload = finalHolder.payload;
            int length = payload.getInt(location);
            return payload.retainedSlice(location + 4, length);
        }

        logger.debug("Segment has no readable message, location={}", location);
        return null;
    }

    public ByteBuf readCompleted(int location) {
        ByteBufHolder finalHolder = holder;
        if (finalHolder != null) {
            ByteBuf payload = finalHolder.payload;
            int length = payload.getInt(location);
            return payload.retainedSlice(location, length + 4);
        }

        logger.debug("Segment has no readable completed message, location={}", location);
        return null;
    }

    public int freeBytes() {
        ByteBufHolder theHolder = holder;
        return theHolder == null ? 0 : theHolder.payload.writableBytes();
    }

    public void tail(Segment segment) {
        this.next = segment;
    }

    public Segment next() {
        return next;
    }

    public boolean isActive() {
        return holder != null;
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

    @SuppressWarnings("unused")
    public int getLogId() {
        return ledgerId;
    }

    /**
     * The location of message sites is based on the storage order,
     * <p>offset</p> need to change the offset of the positioning.
     * {@link Segment#write}
     */
    public int locate(Offset offset) {
        if (offset == null) {
            return tailLocation;
        }

        if (!offset.after(head)) {
            return headLocation;
        }

        ByteBufHolder theHolder = holder;
        if (theHolder == null) {
            return tailLocation;
        }

        ByteBuf theBuf = holder.payload;
        int theEpoch = offset.epoch();
        long theIndex = offset.index();

        int limit = tailLocation;
        int position = headLocation;
        while (position < limit) {
            int length = theBuf.getInt(position);

            int topicLength = theBuf.getInt(position + 6);
            int queueLength = theBuf.getInt(position + 10 + topicLength);

            int epoch = theBuf.getInt(position + 14 + queueLength + topicLength);
            if (epoch > theEpoch) {
                return position;
            } else if (epoch == theEpoch) {
                long index = theBuf.getLong(position + 18 + queueLength + topicLength);
                if (index > theIndex) {
                    return position;
                }
            }

            position += length + 4;
        }

        return limit;
    }

    public void release() {
        logger.debug("Release segment of ledger={} headOffset={} tailOffset={}", ledgerId, head, tail);
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
            for (; ; ) {
                try {
                    Reference<? extends ByteBufHolder> reference = BYTE_BUF_HOLDER_REFERENCE_QUEUE.remove();
                    ByteBuf buf = BYTE_BUF_MAP.remove(reference);

                    if (buf != null) {
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

    @Override
    public String toString() {
        return "Segment{" +
                "ledgerId=" + ledgerId +
                ", head=" + head +
                ", headLocation=" + headLocation +
                ", tail=" + tail +
                ", tailLocation=" + tailLocation +
                ", holder=" + holder +
                ", next=" + next +
                '}';
    }
}
