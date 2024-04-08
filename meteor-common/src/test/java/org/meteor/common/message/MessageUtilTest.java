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

    private static ByteBuf message;

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

    @AfterClass
    public static void Clear() {
        message.release();
    }

    @Test
    public void testGetOffset() {
        Offset offset = MessageUtil.getOffset(message);
        Assert.assertNotNull(offset);
        Assert.assertEquals(offset.getIndex(), 0);
        Assert.assertEquals(offset.getEpoch(), 0);
    }

    @Test
    public void testGetMarker() {
        int marker = MessageUtil.getMarker(message);
        Assert.assertEquals(marker, 1);
    }

    @Test
    public void testGetEpoch() {
        int epoch = MessageUtil.getEpoch(message);
        Assert.assertEquals(0, epoch);
    }

    @Test
    public void testGetIndex() {
        int index = MessageUtil.getIndex(message);
        Assert.assertEquals(index, 0);
    }

    @Test
    public void testGetPayload() {
        ByteBuf buf = MessageUtil.getPayload(message);
        int length = buf.readableBytes();
        String payload = buf.toString(buf.readerIndex(), length, StandardCharsets.UTF_8).trim();
        Assert.assertNotNull(payload);
        Assert.assertEquals(payload, "this is message util test data\u0000\u0000\u0000.");
    }

    @Test
    public void testIsContinuous() {
        boolean continuous = MessageUtil.isContinuous(new Offset(0, 0), new Offset(0, 1));
        Assert.assertTrue(continuous);
    }
}
