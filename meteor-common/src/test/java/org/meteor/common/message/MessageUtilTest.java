package org.meteor.common.message;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.meteor.common.util.MessageUtil;

import java.nio.charset.StandardCharsets;

public class MessageUtilTest {

    /**
     * A static ByteBuf object used to store message data for testing purposes.
     * It is initialized in the setUp method and released in the Clear method.
     * Throughout the tests, it holds serialized data representing a message with
     * an offset, payload, marker, epoch, and index.
     */
    private static ByteBuf message;

    /**
     * Sets up the testing environment before any tests are run.
     * This method initializes a ByteBuf message with a predefined payload,
     * including setting an offset, marker, and total message length.
     * It is annotated with {@code @BeforeClass}, indicating it will be executed once
     * before any test methods in the test class.
     */
    @BeforeClass
    public static void setUp() {
        message = PooledByteBufAllocator.DEFAULT.directBuffer(4194304, 4194304);
        ByteBuf payload = Unpooled.copiedBuffer("this is message util test data", StandardCharsets.UTF_8);
        int length = 16 + payload.readableBytes();

        Offset offset = new Offset(0, 0);
        message.writeInt(1);
        message.writeInt(offset.getEpoch());
        message.writeLong(offset.getIndex());
        message.writeBytes(payload);
        message.writeInt(length);
    }

    /**
     * Releases the allocated ByteBuf resources associated with the message.
     * This method should be called after all tests have been executed to
     * ensure that any memory allocated for the ByteBuf is properly released.
     */
    @AfterClass
    public static void Clear() {
        message.release();
    }

    /**
     * Tests the {@code MessageUtil.getOffset} method to ensure it returns a valid {@code Offset} object.
     * <p>
     * The test verifies the following:
     * - The returned {@code Offset} object is not null.
     * - The {@code Index} value of the {@code Offset} object is correctly retrieved as 0.
     * - The {@code Epoch} value of the {@code Offset} object is correctly retrieved as 0.
     */
    @Test
    public void testGetOffset() {
        Offset offset = MessageUtil.getOffset(message);
        Assert.assertNotNull(offset);
        Assert.assertEquals(0, offset.getIndex());
        Assert.assertEquals(0, offset.getEpoch());
    }

    /**
     * Tests the {@link MessageUtil#getMarker(ByteBuf)} method.
     * <p>
     * This method validates that the correct marker value is retrieved from the initial
     * part of the {@link ByteBuf} buffer. It compares the retrieved marker value to the
     * expected value of 1.
     */
    @Test
    public void testGetMarker() {
        int marker = MessageUtil.getMarker(message);
        Assert.assertEquals(1, marker);
    }

    /**
     * Test method for {@link MessageUtil#getEpoch(ByteBuf)}.
     * <p>
     * This test verifies that the epoch value extracted from the provided
     * ByteBuf is as expected. The expected value for the epoch in this test
     * scenario is 0.
     */
    @Test
    public void testGetEpoch() {
        int epoch = MessageUtil.getEpoch(message);
        Assert.assertEquals(0, epoch);
    }

    /**
     * Tests the {@link MessageUtil#getIndex(ByteBuf)} method to ensure it correctly
     * retrieves the index value from the provided ByteBuf instance.
     * The test checks if the extracted index is equal to 0.
     */
    @Test
    public void testGetIndex() {
        int index = MessageUtil.getIndex(message);
        Assert.assertEquals(0, index);
    }

    /**
     * Tests the `getPayload(ByteBuf)` method of the `MessageUtil` class to ensure it correctly
     * extracts the payload from the given message and verifies its content.
     * <p>
     * The test retrieves the payload from a predefined message buffer, converts it to a UTF-8
     * encoded string, trims whitespace, and asserts that the payload is not null and has the
     * expected content.
     * <p>
     * Preconditions:
     * - The message buffer contains the data "this is message util test data" followed by
     *   three null characters and a period.
     * <p>
     * Assertions:
     * - The extracted payload string is not null.
     * - The extracted payload string matches the expected content: "this is message util test data\u0000\u0000\u0000.".
     */
    @Test
    public void testGetPayload() {
        ByteBuf buf = MessageUtil.getPayload(message);
        int length = buf.readableBytes();
        String payload = buf.toString(buf.readerIndex(), length, StandardCharsets.UTF_8).trim();
        Assert.assertNotNull(payload);
        Assert.assertEquals("this is message util test data\u0000\u0000\u0000.", payload);
    }

    /**
     * Tests the {@link MessageUtil#isContinuous(Offset, Offset)} method to verify if two Offset instances are continuous.
     * Two offsets are considered continuous if they belong to the same epoch and the second offset's index
     * is exactly one more than the first offset's index, or if they belong to consecutive epochs and the second offset's index is 1.
     * <p>
     * This test case uses two Offsets:
     * - Base Offset with epoch 0 and index 0.
     * - Next Offset with epoch 0 and index 1.
     * <p>
     * The method is expected to return {@code true} for continuous offsets.
     */
    @Test
    public void testIsContinuous() {
        boolean continuous = MessageUtil.isContinuous(new Offset(0, 0), new Offset(0, 1));
        Assert.assertTrue(continuous);
    }
}
