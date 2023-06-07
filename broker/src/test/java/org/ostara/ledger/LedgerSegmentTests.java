package org.ostara.ledger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.Assert;
import org.junit.Test;
import org.ostara.common.Offset;
import org.ostara.log.ledger.LedgerConfig;
import org.ostara.log.ledger.LedgerSegment;
import org.ostara.remote.util.ByteBufUtils;

import java.util.UUID;

public class LedgerSegmentTests {

    @Test
    public void testWriteRecord() {
        Offset offset = new Offset(0, 0L);
        LedgerSegmentTest segment = new LedgerSegmentTest(1, allocateBuf(), offset);
        segment.writeRecord(1, offset, buildPayload());
    }

    @Test
    public void testBaseOffset() {
        Offset offset = new Offset(0, 0L);
        LedgerSegmentTest segment = new LedgerSegmentTest(1, allocateBuf(), offset);
        segment.writeRecord(1, offset, buildPayload());

        Offset baseOffset = segment.baseOffset();
        Assert.assertEquals(offset, baseOffset);
    }

    @Test
    public void testLastOffset() {
        Offset offset = new Offset(0, 0L);
        LedgerSegmentTest segment = new LedgerSegmentTest(1, allocateBuf(), offset);
        segment.writeRecord(1, offset, buildPayload());

        Offset lastOffset = segment.lastOffset();
        Assert.assertEquals(offset, lastOffset);
    }

    @Test
    public void testReadRecord() {
        Offset offset = new Offset(0, 0L);
        LedgerSegmentTest segment = new LedgerSegmentTest(1, allocateBuf(), offset);
        segment.writeRecord(1, offset, buildPayload());

        ByteBuf buf = segment.readRecord(0);
        byte[] bytes = ByteBufUtils.buf2Bytes(buf);
        String uuid = new String(bytes);
        System.out.println(uuid);
    }

    @Test
    public void testNext() {
        Offset offset = new Offset(0, 0L);
        LedgerSegmentTest segment = new LedgerSegmentTest(1, allocateBuf(), offset);
        segment.writeRecord(1, offset, buildPayload());

        LedgerSegment next = segment.next();
        Assert.assertNull(next);
    }

    @Test
    public void testActive() {
        Offset offset = new Offset(0, 0L);
        LedgerSegmentTest segment = new LedgerSegmentTest(1, allocateBuf(), offset);

        boolean active = segment.isActive();
        Assert.assertTrue(active);
    }

    @Test
    public void testFreeBytes() {
        Offset offset = new Offset(0, 0L);
        ByteBuf initBuf = allocateBuf();
        LedgerSegmentTest segment = new LedgerSegmentTest(1, initBuf, offset);
        String message = UUID.randomUUID().toString();
        ByteBuf buf = ByteBufUtils.string2Buf(message);
        segment.writeRecord(1, offset, buf);

        int free = segment.freeBytes();
        Assert.assertEquals(free, initBuf.writableBytes());
    }

    @Test
    public void testUsedBytes() {
        Offset offset = new Offset(0, 0L);
        ByteBuf initBuf = allocateBuf();
        LedgerSegmentTest segment = new LedgerSegmentTest(1, initBuf, offset);
        String message = UUID.randomUUID().toString();
        ByteBuf buf = ByteBufUtils.string2Buf(message);
        segment.writeRecord(1, offset, buf);

        int used = segment.usedBytes();
        Assert.assertEquals(used, initBuf.readableBytes());
    }

    @Test
    public void testCapacity() {
        Offset offset = new Offset(0, 0L);
        ByteBuf initBuf = allocateBuf();
        LedgerSegmentTest segment = new LedgerSegmentTest(1, initBuf, offset);
        LedgerConfig config = new LedgerConfig();
        Assert.assertEquals(segment.capacity(), config.segmentBufferCapacity());
    }

    @Test
    public void testLocateIfNull() {
        Offset offset = new Offset(0, 0L);
        ByteBuf initBuf = allocateBuf();
        LedgerSegmentTest segment = new LedgerSegmentTest(1, initBuf, offset);
        int locate = segment.locate(null);
        Assert.assertEquals(locate, segment.basePosition());
    }

    @Test
    public void testLocateIfNotNull() {
        Offset offset = new Offset(0, 0L);
        ByteBuf initBuf = allocateBuf();
        LedgerSegmentTest segment = new LedgerSegmentTest(1, initBuf, offset);
        int locate = segment.locate(new Offset(0, 2L));
        Assert.assertEquals(locate, segment.basePosition());
    }

    private ByteBuf buildPayload() {
        return ByteBufUtils.string2Buf(UUID.randomUUID().toString());
    }

    private ByteBuf allocateBuf() {
        LedgerConfig config = new LedgerConfig();
       return PooledByteBufAllocator.DEFAULT.directBuffer(config.segmentBufferCapacity(), config.segmentBufferCapacity());
    }

    static class LedgerSegmentTest extends LedgerSegment{
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
