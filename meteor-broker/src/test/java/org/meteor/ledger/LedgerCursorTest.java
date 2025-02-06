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

    /**
     * Tests the copy functionality of the LedgerCursor class. This method creates
     * an instance of LedgerStorage and an InnerLedgerSegmentTest segment to initialize
     * a LedgerCursor. It then creates a copy of the LedgerCursor and asserts that the
     * copied LedgerCursor is not the same instance as the original, but has the same
     * position.
     * <p>
     * This test ensures that the copied LedgerCursor has the same state and position
     * as the original cursor and verifies the correct implementation of the copy method.
     */
    @Test
    public void testCopy() {
        LedgerStorage storage = new LedgerStorage(1, "test", 0, new LedgerConfig(),
                NetworkUtil.newEventExecutorGroup(1, "append-record-group").next(),
                new LedgerCursorTest.LedgerTriggerCursorTest());
        ByteBuf buf = allocateBuf();
        LedgerSegmentTest.InnerLedgerSegmentTest segment =
                new LedgerSegmentTest.InnerLedgerSegmentTest(1, buf, new Offset(0, 0L));

        LedgerCursor cursor = new LedgerCursor(storage, segment, 0);
        LedgerCursor copy = cursor.copy();

        Assert.assertNotEquals(copy, cursor);
        Assert.assertEquals(copy.getPosition(), cursor.getPosition());
        buf.release();
        storage.close(null);
    }

    /**
     * This method tests the hasNext functionality of the LedgerCursor class. It ensures that when records
     * are written to a segment, the cursor correctly identifies the presence of subsequent records.
     * <p>
     * The test performs the following actions:
     * 1. Initializes a LedgerStorage instance with appropriate configurations.
     * 2. Allocates a ByteBuf for writing test records.
     * 3. Creates a LedgerSegmentCursorTest instance to manage the segment operations.
     * 4. Writes test records to the segment using specific offsets.
     * 5. Creates a LedgerCursor instance to iterate over the records in the segment.
     * 6. Calls the hasNext method on the cursor to check if another record exists.
     * 7. Asserts that the result of hasNext is true, confirming the presence of a next record.
     * 8. Releases allocated ByteBuf resources.
     * 9. Closes the LedgerStorage instance.
     */
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

    /**
     * This method is a unit test for the next() method of the LedgerCursor class.
     * It sets up a test ledger storage and simulates writing records to a ledger segment.
     * It then creates a LedgerCursor to read the records and asserts that the
     * content read is the same as the content written.
     */
    @Test
    public void testNext() {
        LedgerStorage storage = new LedgerStorage(1, "test", 0, new LedgerConfig(),
                NetworkUtil.newEventExecutorGroup(1, "append-record-group").next(),
                new LedgerCursorTest.LedgerTriggerCursorTest());

        String content = UUID.randomUUID().toString();
        ByteBuf data = ByteBufUtil.string2Buf(content);

        Offset offset = new Offset(0, 0L);
        ByteBuf buf = allocateBuf();
        LedgerSegmentTest.InnerLedgerSegmentTest segment = new LedgerSegmentTest.InnerLedgerSegmentTest(1, buf, offset);
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

    /**
     * Tests the functionality of seeking to a specific offset in a ledger cursor.
     * <p>
     * Sets up a LedgerStorage instance and writes several records to a segment.
     * Then, seeks to a specific offset within the segment using a LedgerCursor.
     * Asserts that the position of the cursor after seeking matches the expected offset.
     */
    @Test
    public void testSeekTo() {
        LedgerStorage storage = new LedgerStorage(1, "test", 0, new LedgerConfig(),
                NetworkUtil.newEventExecutorGroup(1, "append-record-group").next(),
                new LedgerCursorTest.LedgerTriggerCursorTest());

        String content = UUID.randomUUID().toString();
        ByteBuf data = ByteBufUtil.string2Buf(content);
        Offset offset = new Offset(0, 0L);
        ByteBuf buf = allocateBuf();
        LedgerSegmentTest.InnerLedgerSegmentTest segment = new LedgerSegmentTest.InnerLedgerSegmentTest(1, buf, offset);
        segment.writeRecord(1, offset, data);
        segment.writeRecord(1, new Offset(0, 1L), data);
        segment.writeRecord(1, new Offset(0, 2L), data);

        LedgerCursor cursor = new LedgerCursor(storage, segment, 0);
        LedgerCursor seekCursor = cursor.seekTo(new Offset(0, 1L));
        Assert.assertEquals(segment.locate(new Offset(0, 1L)), seekCursor.getPosition());
        buf.release();
        storage.close(null);
    }

    /**
     * Test method for ensuring the correct functionality of the seekToTail operation
     * on a LedgerCursor. This method sets up a ledger environment, writes multiple
     * records to a ledger segment, and then uses a LedgerCursor to seek to the tail
     * of the segment. The method verifies that the cursor's position is accurately
     * set to the last written record.
     *
     */
    @Test
    public void testSeekToTail() {
        LedgerStorage storage = new LedgerStorage(1, "test", 0, new LedgerConfig(),
                NetworkUtil.newEventExecutorGroup(1, "append-record-group").next(),
                new LedgerCursorTest.LedgerTriggerCursorTest());

        String content = UUID.randomUUID().toString();
        ByteBuf data = ByteBufUtil.string2Buf(content);
        Offset offset = new Offset(0, 0L);
        ByteBuf buf = allocateBuf();
        LedgerSegmentTest.InnerLedgerSegmentTest segment = new LedgerSegmentTest.InnerLedgerSegmentTest(1, buf, offset);
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

    /**
     * Generates a payload as a ByteBuf containing a randomly generated UUID string.
     *
     * @return a ByteBuf containing the UUID string.
     */
    private ByteBuf buildPayload() {
        return ByteBufUtil.string2Buf(UUID.randomUUID().toString());
    }

    /**
     * Allocates and returns a direct buffer with a capacity defined by the segment buffer capacity set in the LedgerConfig.
     *
     * @return a ByteBuf instance with the configured segment buffer capacity
     */
    private ByteBuf allocateBuf() {
        LedgerConfig config = new LedgerConfig();
        return PooledByteBufAllocator.DEFAULT.directBuffer(config.segmentBufferCapacity(), config.segmentBufferCapacity());
    }

    static class LedgerTriggerCursorTest implements LedgerTrigger {
        @Override
        public void onAppend(int ledgerId, int recordCount, Offset lasetOffset) {
            Assert.assertEquals(1, ledgerId);
            Assert.assertEquals(1, recordCount);
            Assert.assertEquals(0, lasetOffset.getEpoch());
            Assert.assertEquals(1, lasetOffset.getIndex());
        }

        @Override
        public void onRelease(int ledgerId, Offset oldHeadOffset, Offset newHeadOffset) {
            // do nothing
        }
    }

    static class LedgerSegmentCursorTest extends LedgerSegment {
        /**
         * Initializes a new instance of the LedgerSegmentCursorTest class.
         *
         * @param ledger the ledger identifier.
         * @param buffer the byte buffer to hold data.
         * @param baseOffset the base offset for the segment.
         */
        public LedgerSegmentCursorTest(int ledger, ByteBuf buffer, Offset baseOffset) {
            super(ledger, buffer, baseOffset);
        }

        /**
         * Writes a record to the buffer with a specified marker, offset, and payload.
         *
         * @param marker an integer marker used to identify the type of record
         * @param offset the Offset object indicating the epoch and index for the record location
         * @param payload the ByteBuf containing the data to be written as the record's payload
         */
        @Override
        protected void writeRecord(int marker, Offset offset, ByteBuf payload) {
            super.writeRecord(marker, offset, payload);
        }

        /**
         * Reads a record from the buffer at a specified position.
         *
         * @param position the starting position in the buffer to read the record from.
         * @return the record as a ByteBuf, or null if the record could not be read.
         */
        @Override
        protected ByteBuf readRecord(int position) {
            return super.readRecord(position);
        }

        /**
         * Locates the position of the given offset within the ledger segment.
         *
         * @param offset the target offset to locate; if null, returns the last position.
         * @return the position of the given offset within the ledger segment,
         *         or the base position if the offset is before or equal to the base offset,
         *         or the last position if no matching entry is found.
         */
        @Override
        protected int locate(Offset offset) {
            return super.locate(offset);
        }

        /**
         * Retrieves the base offset of the ledger segment.
         *
         * @return the base offset of this ledger segment.
         */
        @Override
        public Offset baseOffset() {
            return super.baseOffset();
        }

        /**
         * Returns the base position of the ledger segment.
         *
         * @return the base position of the ledger segment
         */
        @Override
        public int basePosition() {
            return super.basePosition();
        }

        /**
         * Retrieves the last offset in the ledger segment.
         *
         * @return the last offset recorded in this ledger segment.
         */
        @Override
        public Offset lastOffset() {
            return super.lastOffset();
        }

        /**
         * Returns the last position within the ledger segment.
         *
         * @return the last position in the ledger segment
         */
        @Override
        public int lastPosition() {
            return super.lastPosition();
        }

        /**
         * Retrieves the next {@link LedgerSegment}.
         *
         * @return the next {@link LedgerSegment}.
         */
        @Override
        public LedgerSegment next() {
            return super.next();
        }

        /**
         * Sets the next LedgerSegment.
         *
         * @param next the next LedgerSegment to be set
         */
        @Override
        public void next(LedgerSegment next) {
            super.next(next);
        }

        /**
         * Checks if the current ledger segment is active.
         *
         * @return true if the current ledger segment is active; false otherwise.
         */
        @Override
        protected boolean isActive() {
            return super.isActive();
        }

        /**
         * Releases resources held by this LedgerSegmentCursorTest instance.
         * <p>
         * Overrides the release method in the superclass to ensure that
         * any resources or references held by this instance are properly
         * cleaned up when it is no longer needed.
         */
        @Override
        protected void release() {
            super.release();
        }

        /**
         * Calculates the number of writable bytes remaining in the buffer held by this object.
         *
         * @return the number of writable bytes if the buffer exists, otherwise 0.
         */
        @Override
        protected int freeBytes() {
            return super.freeBytes();
        }

        /**
         * Calculates the number of bytes currently used in the buffer.
         *
         * @return the number of readable bytes in the buffer if available; 0 otherwise.
         */
        @Override
        protected int usedBytes() {
            return super.usedBytes();
        }

        /**
         * Determines the capacity of the buffer held by the current BufferHolder.
         *
         * @return the capacity of the buffer if the BufferHolder is not null;
         *         otherwise, returns 0.
         */
        @Override
        protected int capacity() {
            return super.capacity();
        }
    }
}
