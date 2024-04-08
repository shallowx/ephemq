package org.meteor.ledger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.Assert;
import org.junit.Test;
import org.meteor.common.message.Offset;
import org.meteor.common.util.MessageUtil;
import org.meteor.remote.util.ByteBufUtil;
import org.meteor.remote.util.NetworkUtil;

import java.util.UUID;

public class LedgerCursorTest {

    @Test
    public void testCopy() {
        LedgerStorage storage = new LedgerStorage(1, "test", 0, new LedgerConfig(),
                NetworkUtil.newEventExecutorGroup(1, "append-record-group").next(),
                new LedgerCursorTest.LedgerTriggerCursorTest());
        ByteBuf buf = allocateBuf();
        LedgerSegmentTest.LedgerSegmentTest segment =
                new LedgerSegmentTest.LedgerSegmentTest(1, buf, new Offset(0, 0L));

        LedgerCursor cursor = new LedgerCursor(storage, segment, 0);
        LedgerCursor copy = cursor.copy();

        Assert.assertNotEquals(copy, cursor);
        Assert.assertEquals(copy.getPosition(), cursor.getPosition());
        buf.release();
        storage.close(null);
    }

    @Test
    public void testHasNext() {
        LedgerStorage storage = new LedgerStorage(1, "test", 0, new LedgerConfig(),
                NetworkUtil.newEventExecutorGroup(1, "append-record-group").next(),
                new LedgerCursorTest.LedgerTriggerCursorTest());

        Offset offset = new Offset(0, 0L);
        ByteBuf buf = allocateBuf();
        LedgerCursorTest.LedgerSegmentCursorTest segment = new LedgerCursorTest.LedgerSegmentCursorTest(1, buf, offset);
        segment.writeRecord(1, offset, buildPayload());
        segment.writeRecord(1, new Offset(0, 1L), buildPayload());

        LedgerCursor cursor = new LedgerCursor(storage, segment, 0);
        boolean exists = cursor.hashNext();
        Assert.assertTrue(exists);
        buf.release();
        storage.close(null);
    }

    @Test
    public void testNext() {
        LedgerStorage storage = new LedgerStorage(1, "test", 0, new LedgerConfig(),
                NetworkUtil.newEventExecutorGroup(1, "append-record-group").next(),
                new LedgerCursorTest.LedgerTriggerCursorTest());

        String content = UUID.randomUUID().toString();
        ByteBuf data = ByteBufUtil.string2Buf(content);

        Offset offset = new Offset(0, 0L);
        ByteBuf buf = allocateBuf();
        LedgerSegmentTest.LedgerSegmentTest segment = new LedgerSegmentTest.LedgerSegmentTest(1, buf, offset);
        segment.writeRecord(1, offset, data);
        segment.writeRecord(1, new Offset(0, 1L), data);

        LedgerCursor cursor = new LedgerCursor(storage, segment, 0);
        ByteBuf next = cursor.next();
        ByteBuf payload = MessageUtil.getPayload(next);
        String message = ByteBufUtil.buf2String(payload, 4 * 1024 * 1024);
        Assert.assertEquals(message, content);

        buf.release();
        next.release();
        storage.close(null);
    }

    @Test
    public void testSeekTo() {
        LedgerStorage storage = new LedgerStorage(1, "test", 0, new LedgerConfig(),
                NetworkUtil.newEventExecutorGroup(1, "append-record-group").next(),
                new LedgerCursorTest.LedgerTriggerCursorTest());

        String content = UUID.randomUUID().toString();
        ByteBuf data = ByteBufUtil.string2Buf(content);
        Offset offset = new Offset(0, 0L);
        ByteBuf buf = allocateBuf();
        LedgerSegmentTest.LedgerSegmentTest segment = new LedgerSegmentTest.LedgerSegmentTest(1, buf, offset);
        segment.writeRecord(1, offset, data);
        segment.writeRecord(1, new Offset(0, 1L), data);
        segment.writeRecord(1, new Offset(0, 2L), data);

        LedgerCursor cursor = new LedgerCursor(storage, segment, 0);
        LedgerCursor seekCursor = cursor.seekTo(new Offset(0, 1L));
        Assert.assertEquals(segment.locate(new Offset(0, 1L)), seekCursor.getPosition());
        buf.release();
        storage.close(null);
    }

    @Test
    public void testSeekToTail() {
        LedgerStorage storage = new LedgerStorage(1, "test", 0, new LedgerConfig(),
                NetworkUtil.newEventExecutorGroup(1, "append-record-group").next(),
                new LedgerCursorTest.LedgerTriggerCursorTest());

        String content = UUID.randomUUID().toString();
        ByteBuf data = ByteBufUtil.string2Buf(content);
        Offset offset = new Offset(0, 0L);
        ByteBuf buf = allocateBuf();
        LedgerSegmentTest.LedgerSegmentTest segment = new LedgerSegmentTest.LedgerSegmentTest(1, buf, offset);
        segment.writeRecord(1, offset, data);
        segment.writeRecord(1, new Offset(0, 1L), data);
        segment.writeRecord(1, new Offset(0, 2L), data);

        LedgerCursor cursor = new LedgerCursor(storage, segment, 0);
        LedgerCursor seekCursor = cursor.seekToTail();

        Assert.assertEquals(segment.locate(new Offset(0, 0L)), seekCursor.getPosition());
        Assert.assertEquals(segment.locate(new Offset(0, 2L)), segment.lastPosition());
        buf.release();
        storage.close(null);
    }

    private ByteBuf buildPayload() {
        return ByteBufUtil.string2Buf(UUID.randomUUID().toString());
    }

    private ByteBuf allocateBuf() {
        LedgerConfig config = new LedgerConfig();
        return PooledByteBufAllocator.DEFAULT.directBuffer(config.segmentBufferCapacity(), config.segmentBufferCapacity());
    }

    static class LedgerTriggerCursorTest implements LedgerTrigger {
        @Override
        public void onAppend(int ledgerId, int recordCount, Offset lasetOffset) {
            Assert.assertEquals(ledgerId, 1);
            Assert.assertEquals(recordCount, 1);
            Assert.assertEquals(lasetOffset.getEpoch(), 0);
            Assert.assertEquals(lasetOffset.getIndex(), 1);
        }

        @Override
        public void onRelease(int ledgerId, Offset oldHeadOffset, Offset newHeadOffset) {
            // do nothing
        }
    }

    static class LedgerSegmentCursorTest extends LedgerSegment {
        public LedgerSegmentCursorTest(int ledger, ByteBuf buffer, Offset baseOffset) {
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
