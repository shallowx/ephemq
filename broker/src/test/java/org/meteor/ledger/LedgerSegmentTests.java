package org.meteor.ledger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.Assert;
import org.junit.Test;
import org.meteor.common.message.Offset;
import org.meteor.common.internal.MessageUtil;
import org.meteor.remote.util.ByteBufUtil;

import java.util.UUID;

public class LedgerSegmentTests {

    @Test
    public void testWriteRecord() {
        Offset offset = new Offset(0, 0L);
        ByteBuf buf = allocateBuf();
        LedgerSegmentTest segment = new LedgerSegmentTest(1, buf, offset);
        segment.writeRecord(1, offset, buildPayload());
        buf.release();
    }

    @Test
    public void testBaseOffset() {
        Offset offset = new Offset(0, 0L);
        ByteBuf buf = allocateBuf();
        LedgerSegmentTest segment = new LedgerSegmentTest(1, buf, offset);
        ByteBuf payload = buildPayload();
        segment.writeRecord(1, offset, payload);

        Offset baseOffset = segment.baseOffset();
        buf.release();
        payload.release();
        Assert.assertEquals(offset, baseOffset);
    }

    @Test
    public void testLastOffset() {
        Offset offset = new Offset(0, 0L);
        ByteBuf buf = allocateBuf();
        LedgerSegmentTest segment = new LedgerSegmentTest(1, buf, offset);
        ByteBuf payload = buildPayload();
        segment.writeRecord(1, offset, payload);

        Offset lastOffset = segment.lastOffset();
        buf.release();
        payload.release();
        Assert.assertEquals(offset, lastOffset);
    }

    @Test
    public void testReadRecord() {
        Offset offset = new Offset(0, 0L);
        ByteBuf buf = allocateBuf();
        LedgerSegmentTest segment = new LedgerSegmentTest(1, buf, offset);
        String content = UUID.randomUUID().toString();
        ByteBuf data = ByteBufUtil.string2Buf(content);

        segment.writeRecord(1, offset, data);

        ByteBuf record = segment.readRecord(0);
        ByteBuf payload = MessageUtil.getPayload(record);
        byte[] bytes = ByteBufUtil.buf2Bytes(payload);
        String uuid = new String(bytes);

        buf.release();
        data.release();
        record.release();
        Assert.assertEquals(content, uuid);
    }

    @Test
    public void testNext() {
        Offset offset = new Offset(0, 0L);
        ByteBuf buf = allocateBuf();
        LedgerSegmentTest segment = new LedgerSegmentTest(1, buf, offset);
        ByteBuf payload = buildPayload();
        segment.writeRecord(1, offset, payload);

        LedgerSegment next = segment.next();
        buf.release();
        payload.release();
        Assert.assertNull(next);
    }

    @Test
    public void testActive() {
        Offset offset = new Offset(0, 0L);
        ByteBuf buf = allocateBuf();
        LedgerSegmentTest segment = new LedgerSegmentTest(1, buf, offset);

        boolean active = segment.isActive();
        buf.release();
        Assert.assertTrue(active);
    }

    @Test
    public void testFreeBytes() {
        Offset offset = new Offset(0, 0L);
        ByteBuf initBuf = allocateBuf();
        LedgerSegmentTest segment = new LedgerSegmentTest(1, initBuf, offset);
        String message = UUID.randomUUID().toString();
        ByteBuf buf = ByteBufUtil.string2Buf(message);
        segment.writeRecord(1, offset, buf);

        int free = segment.freeBytes();
        Assert.assertEquals(free, initBuf.writableBytes());

        initBuf.release();
        buf.release();
    }

    @Test
    public void testUsedBytes() {
        Offset offset = new Offset(0, 0L);
        ByteBuf initBuf = allocateBuf();
        LedgerSegmentTest segment = new LedgerSegmentTest(1, initBuf, offset);
        String message = UUID.randomUUID().toString();
        ByteBuf buf = ByteBufUtil.string2Buf(message);
        segment.writeRecord(1, offset, buf);

        int used = segment.usedBytes();
        buf.release();

        Assert.assertEquals(used, initBuf.readableBytes());
        initBuf.release();
    }

    @Test
    public void testCapacity() {
        Offset offset = new Offset(0, 0L);
        ByteBuf initBuf = allocateBuf();
        LedgerSegmentTest segment = new LedgerSegmentTest(1, initBuf, offset);
        LedgerConfig config = new LedgerConfig();
        Assert.assertEquals(segment.capacity(), config.segmentBufferCapacity());
        initBuf.release();
    }

    @Test
    public void testLocateIfNull() {
        Offset offset = new Offset(0, 0L);
        ByteBuf initBuf = allocateBuf();
        LedgerSegmentTest segment = new LedgerSegmentTest(1, initBuf, offset);
        int locate = segment.locate(null);
        Assert.assertEquals(locate, segment.basePosition());
        initBuf.release();
    }

    @Test
    public void testLocateIfNotNull() {
        Offset offset = new Offset(0, 0L);
        ByteBuf initBuf = allocateBuf();
        LedgerSegmentTest segment = new LedgerSegmentTest(1, initBuf, offset);
        int locate = segment.locate(new Offset(0, 2L));
        Assert.assertEquals(locate, segment.basePosition());
        initBuf.release();
    }

    private ByteBuf buildPayload() {
        return ByteBufUtil.string2Buf(UUID.randomUUID().toString());
    }

    private ByteBuf allocateBuf() {
        LedgerConfig config = new LedgerConfig();
        return PooledByteBufAllocator.DEFAULT.directBuffer(config.segmentBufferCapacity(), config.segmentBufferCapacity());
    }

    static class LedgerSegmentTest extends LedgerSegment {
        public LedgerSegmentTest(int ledger, ByteBuf buffer, Offset baseOffset) {
            super(ledger, buffer, baseOffset);
        }

        @Override
        protected void writeRecord(int marker, Offset offset, ByteBuf payload) {
            super.writeRecord(marker, offset, payload);
        }

        @Override
        protected ByteBuf readRecord(int position) {
            return super.readRecord(position);
        }

        @Override
        protected int locate(Offset offset) {
            return super.locate(offset);
        }

        @Override
        public Offset baseOffset() {
            return super.baseOffset();
        }

        @Override
        public int basePosition() {
            return super.basePosition();
        }

        @Override
        public Offset lastOffset() {
            return super.lastOffset();
        }

        @Override
        public int lastPosition() {
            return super.lastPosition();
        }

        @Override
        public LedgerSegment next() {
            return super.next();
        }

        @Override
        public void next(LedgerSegment next) {
            super.next(next);
        }

        @Override
        protected boolean isActive() {
            return super.isActive();
        }

        @Override
        protected void release() {
            super.release();
        }

        @Override
        protected int freeBytes() {
            return super.freeBytes();
        }

        @Override
        protected int usedBytes() {
            return super.usedBytes();
        }

        @Override
        protected int capacity() {
            return super.capacity();
        }
    }
}
