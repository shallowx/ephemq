package org.meteor.ledger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.Assert;
import org.junit.Test;
import org.meteor.common.message.Offset;
import org.meteor.common.util.MessageUtil;
import org.meteor.remote.util.ByteBufUtil;

import java.util.UUID;

public class LedgerSegmentTest {

    /**
     * Test method for writing a record to the ledger segment.
     * <p>
     * This method initializes the necessary components including an offset, buffer allocation,
     * and an instance of InnerLedgerSegmentTest. It then writes a record to the segment using
     * these components and releases the buffer.
     */
    @Test
    public void testWriteRecord() {
        Offset offset = new Offset(0, 0L);
        ByteBuf buf = allocateBuf();
        InnerLedgerSegmentTest segment = new InnerLedgerSegmentTest(1, buf, offset);
        segment.writeRecord(1, offset, buildPayload());
        buf.release();
    }

    /**
     * Tests the {@code baseOffset} method of the {@code InnerLedgerSegmentTest} class.
     * <p>
     * This method verifies that the base offset of a ledger segment is correctly
     * initialized and retrieved after writing a record to the segment.
     * <p>
     * The test performs the following steps:
     * 1. Initializes an {@code Offset} with epoch 0 and index 0.
     * 2. Allocates a new {@code ByteBuf} for the segment buffer.
     * 3. Creates an {@code InnerLedgerSegmentTest} instance with the given offset and buffer.
     * 4. Allocates a payload using the {@code buildPayload} method.
     * 5. Writes a record to the segment using the {@code writeRecord} method.
     * 6. Asserts that the base offset of the segment is equal to the initially given offset.
     * 7. Releases the allocated buffers to prevent memory leaks.
     */
    @Test
    public void testBaseOffset() {
        Offset offset = new Offset(0, 0L);
        ByteBuf buf = allocateBuf();
        InnerLedgerSegmentTest segment = new InnerLedgerSegmentTest(1, buf, offset);
        ByteBuf payload = buildPayload();
        segment.writeRecord(1, offset, payload);

        Offset baseOffset = segment.baseOffset();
        buf.release();
        payload.release();
        Assert.assertEquals(offset, baseOffset);
    }

    /**
     * Tests the behavior of the method {@code lastOffset()} in the {@code InnerLedgerSegmentTest} class.
     * <p>
     * This test ensures that after a record is written to the segment, the last offset reported by
     * {@code lastOffset()} matches the offset of the written record.
     * <p>
     * The test steps are as follows:
     * 1. Creates an {@code Offset} instance with epoch 0 and index 0.
     * 2. Allocates a {@code ByteBuf} buffer using {@code allocateBuf()}.
     * 3. Instantiates an {@code InnerLedgerSegmentTest} object using the allocated buffer and initial offset.
     * 4. Builds a payload using {@code buildPayload()}.
     * 5. Writes a record to the segment at the specified offset and payload.
     * 6. Retrieves the last offset through {@code lastOffset()}.
     * 7. Releases the buffers to free up resources.
     * 8. Asserts that the initial offset is equal to the retrieved last offset.
     */
    @Test
    public void testLastOffset() {
        Offset offset = new Offset(0, 0L);
        ByteBuf buf = allocateBuf();
        InnerLedgerSegmentTest segment = new InnerLedgerSegmentTest(1, buf, offset);
        ByteBuf payload = buildPayload();
        segment.writeRecord(1, offset, payload);

        Offset lastOffset = segment.lastOffset();
        buf.release();
        payload.release();
        Assert.assertEquals(offset, lastOffset);
    }

    /**
     * Tests the readRecord method of the InnerLedgerSegmentTest class.
     * <p>
     * The method performs the following:
     * 1. Allocates a ByteBuf buffer.
     * 2. Initializes an InnerLedgerSegmentTest instance with a base offset.
     * 3. Generates a random UUID string and converts it to a ByteBuf.
     * 4. Writes the UUID payload to the segment.
     * 5. Reads the record back from the segment.
     * 6. Extracts the payload from the read record.
     * 7. Converts the payload to a string.
     * 8. Releases all allocated ByteBuf objects.
     * 9. Asserts that the original UUID string matches the string extracted
     *    from the read record payload.
     */
    @Test
    public void testReadRecord() {
        Offset offset = new Offset(0, 0L);
        ByteBuf buf = allocateBuf();
        InnerLedgerSegmentTest segment = new InnerLedgerSegmentTest(1, buf, offset);
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

    /**
     * Tests the behavior of the `next` method in the `InnerLedgerSegmentTest` class.
     *
     * <p>This test instantiates an `InnerLedgerSegmentTest` with an initial offset and buffer.
     * It writes a record to the segment, invokes the `next` method, and asserts that it returns null.
     *
     * <p>The test performs the following operations:
     * <ul>
     *   <li>Initializes an `Offset` object.
     *   <li>Allocates a `ByteBuf` for the segment.
     *   <li>Creates an `InnerLedgerSegmentTest` instance.
     *   <li>Builds a payload and writes a record to the segment.
     *   <li>Calls the `next` method and asserts that the result is null.
     *   <li>Releases the allocated buffers.
     * </ul>
     *
     * <p>Expected outcome: The `next` method returns `null` indicating that there is no next segment.
     */
    @Test
    public void testNext() {
        Offset offset = new Offset(0, 0L);
        ByteBuf buf = allocateBuf();
        InnerLedgerSegmentTest segment = new InnerLedgerSegmentTest(1, buf, offset);
        ByteBuf payload = buildPayload();
        segment.writeRecord(1, offset, payload);

        LedgerSegment next = segment.next();
        buf.release();
        payload.release();
        Assert.assertNull(next);
    }

    /**
     * Tests the functionality of the {@code isActive} method in the {@code InnerLedgerSegmentTest} class.
     * <p>
     * The method verifies if a newly created {@code InnerLedgerSegmentTest} instance
     * with a specific {@code Offset} and buffer is active, and asserts that it is.
     * <p>
     * Steps:
     * 1. Creates a new {@code Offset} instance with initial values.
     * 2. Allocates a buffer for the {@code InnerLedgerSegmentTest} instance.
     * 3. Instantiates {@code InnerLedgerSegmentTest} with the created offset and buffer.
     * 4. Calls the {@code isActive} method on the segment to check its active status.
     * 5. Releases the allocated buffer.
     * 6. Asserts that the segment is active.
     */
    @Test
    public void testActive() {
        Offset offset = new Offset(0, 0L);
        ByteBuf buf = allocateBuf();
        InnerLedgerSegmentTest segment = new InnerLedgerSegmentTest(1, buf, offset);

        boolean active = segment.isActive();
        buf.release();
        Assert.assertTrue(active);
    }

    /**
     * Tests the method {@code freeBytes()} of the {@code InnerLedgerSegmentTest} class.
     * This method creates an initial buffer with a specific capacity, writes a record to it,
     * and then verifies that the number of free bytes remaining in the buffer is as expected.
     * The test ensures that the {@code freeBytes()} method correctly returns the amount of
     * unused space in the buffer after a record has been written.
     * <p>
     * The test performs the following steps:
     * 1. Initializes the offset and allocates an initial buffer.
     * 2. Creates an instance of {@code InnerLedgerSegmentTest} with the initial buffer.
     * 3. Generates a random message and converts it to a {@code ByteBuf}.
     * 4. Writes the message to the segment using the {@code writeRecord} method.
     * 5. Retrieves the number of free bytes using the {@code freeBytes} method.
     * 6. Asserts that the number of free bytes equals the writable bytes of the initial buffer.
     * 7. Releases the allocated buffers.
     */
    @Test
    public void testFreeBytes() {
        Offset offset = new Offset(0, 0L);
        ByteBuf initBuf = allocateBuf();
        InnerLedgerSegmentTest segment = new InnerLedgerSegmentTest(1, initBuf, offset);
        String message = UUID.randomUUID().toString();
        ByteBuf buf = ByteBufUtil.string2Buf(message);
        segment.writeRecord(1, offset, buf);

        int free = segment.freeBytes();
        Assert.assertEquals(free, initBuf.writableBytes());

        initBuf.release();
        buf.release();
    }

    /**
     * Tests the usedBytes method of the InnerLedgerSegmentTest class to ensure it correctly returns the number
     * of bytes used in the segment. This test:
     * <p>
     * 1. Initializes an offset and a byte buffer.
     * 2. Creates an InnerLedgerSegmentTest instance with the given buffer and offset.
     * 3. Writes a record into the segment.
     * 4. Computes the number of used bytes in the segment.
     * 5. Asserts that the number of used bytes matches the expected value.
     */
    @Test
    public void testUsedBytes() {
        Offset offset = new Offset(0, 0L);
        ByteBuf initBuf = allocateBuf();
        InnerLedgerSegmentTest segment = new InnerLedgerSegmentTest(1, initBuf, offset);
        String message = UUID.randomUUID().toString();
        ByteBuf buf = ByteBufUtil.string2Buf(message);
        segment.writeRecord(1, offset, buf);

        int used = segment.usedBytes();
        buf.release();

        Assert.assertEquals(used, initBuf.readableBytes());
        initBuf.release();
    }

    /**
     * Tests the capacity of an `InnerLedgerSegmentTest` instance and verifies that it matches
     * the segment buffer capacity defined in the `LedgerConfig`.
     *
     * <p>This method performs the following steps:
     * <ol>
     * <li>Creates an `Offset` instance with an epoch of 0 and index of 0.</li>
     * <li>Allocates a `ByteBuf` instance.</li>
     * <li>Instantiates an `InnerLedgerSegmentTest` object with the allocated buffer and the `Offset` instance.</li>
     * <li>Creates a `LedgerConfig` instance.</li>
     * <li>Asserts that the `capacity` of the `InnerLedgerSegmentTest` instance is equal to the segment buffer capacity defined in the `LedgerConfig`.</li>
     * <li>Releases the allocated `ByteBuf`.</li>
     * </ol>
     *
     * <p>This method is annotated with `@Test` to indicate that it is a test case to be run by a testing framework like JUnit.
     */
    @Test
    public void testCapacity() {
        Offset offset = new Offset(0, 0L);
        ByteBuf initBuf = allocateBuf();
        InnerLedgerSegmentTest segment = new InnerLedgerSegmentTest(1, initBuf, offset);
        LedgerConfig config = new LedgerConfig();
        Assert.assertEquals(segment.capacity(), config.segmentBufferCapacity());
        initBuf.release();
    }

    /**
     * Tests the {@code locate} method of the {@code InnerLedgerSegmentTest} class when a {@code null} offset is provided.
     * This test verifies that the method returns the base position of the segment when the offset is {@code null}.
     * <p>
     * The method initializes an {@code Offset} object with an epoch and index both set to zero, allocates a buffer,
     * and creates an instance of {@code InnerLedgerSegmentTest} with the aforementioned offset and buffer.
     * It then calls the {@code locate} method with a {@code null} offset and checks if the returned value
     * matches the base position of the segment.
     * <p>
     * It also ensures the allocated buffer is released after the test.
     */
    @Test
    public void testLocateIfNull() {
        Offset offset = new Offset(0, 0L);
        ByteBuf initBuf = allocateBuf();
        InnerLedgerSegmentTest segment = new InnerLedgerSegmentTest(1, initBuf, offset);
        int locate = segment.locate(null);
        Assert.assertEquals(locate, segment.basePosition());
        initBuf.release();
    }

    /**
     * Unit test for the {@code locate} method in the {@code InnerLedgerSegmentTest} class.
     * This test verifies that the locate operation correctly identifies the base position
     * of a segment when provided with a non-null offset.
     * <p>
     * <b>Setup:</b>
     * 1. Creates an {@code Offset} instance initialized to 0 epoch and 0 index.
     * 2. Allocates a {@code ByteBuf} buffer of default segment capacity.
     * 3. Initializes an {@code InnerLedgerSegmentTest} instance with the buffer and base offset.
     * <p>
     * <b>Execution:</b>
     * Calls the {@code locate} method with an offset of epoch 0 and index 2.
     * <p>
     * <b>Validation:</b>
     * Asserts that the located position returned by the {@code locate} method is equal to the base position of the segment.
     * Finally, releases the buffer to ensure there are no memory leaks.
     */
    @Test
    public void testLocateIfNotNull() {
        Offset offset = new Offset(0, 0L);
        ByteBuf initBuf = allocateBuf();
        InnerLedgerSegmentTest segment = new InnerLedgerSegmentTest(1, initBuf, offset);
        int locate = segment.locate(new Offset(0, 2L));
        Assert.assertEquals(locate, segment.basePosition());
        initBuf.release();
    }

    /**
     * Builds a payload ByteBuf by generating a random UUID and converting it to a ByteBuf.
     *
     * @return ByteBuf containing the UTF-8 encoded string of a randomly generated UUID.
     */
    private ByteBuf buildPayload() {
        return ByteBufUtil.string2Buf(UUID.randomUUID().toString());
    }

    /**
     * Allocates a direct buffer with a capacity specified by the current ledger configuration.
     *
     * @return a newly allocated direct ByteBuf with the specified capacity.
     */
    private ByteBuf allocateBuf() {
        LedgerConfig config = new LedgerConfig();
        return PooledByteBufAllocator.DEFAULT.directBuffer(config.segmentBufferCapacity(), config.segmentBufferCapacity());
    }

    static class InnerLedgerSegmentTest extends LedgerSegment {
        /**
         * Constructs an instance of `InnerLedgerSegmentTest` with the specified ledger identifier, byte buffer, and base offset.
         *
         * @param ledger the ledger identifier.
         * @param buffer the byte buffer to hold data.
         * @param baseOffset the base offset for the segment.
         */
        public InnerLedgerSegmentTest(int ledger, ByteBuf buffer, Offset baseOffset) {
            super(ledger, buffer, baseOffset);
        }

        /**
         * Writes a record to the buffer with the specified marker, offset, and payload.
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
         * Locates the position of the specified offset within the ledger segment.
         *
         * @param offset the {@code Offset} to locate; if {@code null}, the method returns the last position.
         * @return the position of the given {@code Offset} within the ledger segment,
         *         or the base position if the offset is before or equal to the base offset,
         *         or the last position if no matching entry is found.
         */
        @Override
        protected int locate(Offset offset) {
            return super.locate(offset);
        }

        /**
         * Retrieves the base offset of the current ledger segment.
         *
         * @return the base offset of this ledger segment.
         */
        @Override
        public Offset baseOffset() {
            return super.baseOffset();
        }

        /**
         * Returns the base position of the ledger segment by delegating the call to the superclass method.
         *
         * @return the base position of the ledger segment
         */
        @Override
        public int basePosition() {
            return super.basePosition();
        }

        /**
         * Retrieves the last offset recorded in the ledger segment.
         *
         * @return the last offset in the ledger segment.
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
         * Retrieves the next ledger segment.
         *
         * @return the next LedgerSegment in the sequence.
         */
        @Override
        public LedgerSegment next() {
            return super.next();
        }

        /**
         * Sets the next ledger segment in the chain.
         *
         * @param next the next {@code LedgerSegment} to set
         */
        @Override
        public void next(LedgerSegment next) {
            super.next(next);
        }

        /**
         * Checks if the current ledger segment is active.
         *
         * @return true if the current segment is active; false otherwise.
         */
        @Override
        protected boolean isActive() {
            return super.isActive();
        }

        /**
         * Releases resources held by the InnerLedgerSegmentTest instance.
         * <p>
         * This method invokes the release method from the parent class, LedgerSegment,
         * to ensure proper cleanup of resources such as memory and associated objects.
         * It is intended to be called when the InnerLedgerSegmentTest instance is no longer needed.
         */
        @Override
        protected void release() {
            super.release();
        }

        /**
         * Calculates the number of writable bytes remaining in the buffer held by the phantomHolder.
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
         * @return the number of readable bytes in the buffer.
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
