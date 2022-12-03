package org.ostara.atomic;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.nio.charset.StandardCharsets;
import org.junit.Assert;
import org.junit.Test;
import org.ostara.ledger.Offset;
import org.ostara.ledger.Segment;
import org.ostara.remote.util.ByteBufUtils;

public class SegmentTests {

    private final String topic = "test-topic";
    private final String queue = "test-queue";
    private final ByteBuf payload = ByteBufUtils.byte2Buf("testWriteBuf".getBytes(StandardCharsets.UTF_8));

    @Test
    public void testWriteBuf() {
        int bytes = topic.length() + queue.length() + 26 + payload.readableBytes();
        Segment segment = new Segment(0, PooledByteBufAllocator.DEFAULT.directBuffer(bytes, bytes), new Offset(-1, 0));
        segment.write(topic, queue, (short) 0, payload, new Offset(-1, 1));

        ByteBufUtils.release(payload);
        segment.release();
    }

    @Test
    public void testReadBuf() {
        int bytes = topic.length() + queue.length() + 26 + payload.readableBytes();
        Segment segment = new Segment(0, PooledByteBufAllocator.DEFAULT.directBuffer(bytes, bytes), new Offset(-1, 0));
        segment.write(topic, queue, (short) 0, payload, new Offset(-1, 1));

        ByteBuf buf = segment.read(segment.headLocation());

        short version = buf.readShort();

        int topicLength = buf.readInt();
        ByteBuf topicBuf = buf.retainedSlice(buf.readerIndex(), topicLength);
        String topic = ByteBufUtils.buf2String(topicBuf, topicLength);


        Assert.assertEquals(topic, topic);
        Assert.assertEquals(0, version);

        ByteBufUtils.release(topicBuf);
        ByteBufUtils.release(payload);

        segment.release();
    }

    @Test
    public void testReadCompletedBuf() {
        int bytes = topic.length() + queue.length() + 26 + payload.readableBytes();
        Segment segment = new Segment(0, PooledByteBufAllocator.DEFAULT.directBuffer(bytes, bytes), new Offset(-1, 0));
        segment.write(topic, queue, (short) 0, payload, new Offset(-1, 1));

        ByteBuf buf = segment.readCompleted(segment.headLocation());

        buf.skipBytes(4);
        short version = buf.readShort();

        int topicLength = buf.readInt();
        ByteBuf topicBuf = buf.retainedSlice(buf.readerIndex(), topicLength);
        String topic = ByteBufUtils.buf2String(topicBuf, topicLength);


        Assert.assertEquals(topic, topic);
        Assert.assertEquals(0, version);

        ByteBufUtils.release(topicBuf);
        ByteBufUtils.release(payload);

        segment.release();
    }

    @Test
    public void testLocate() {
        int bytes = topic.length() + queue.length() + 26 + payload.readableBytes();

        Segment segment =
                new Segment(0, PooledByteBufAllocator.DEFAULT.directBuffer(bytes * 10, bytes * 10), new Offset(-1, 0));

        for (int i = 0; i < 10; i++) {
            payload.retain();
            segment.write(topic, queue, (short) 0, payload, new Offset(-1, i + 1));
        }

        int locate = segment.locate(new Offset(-1, 5));
        Assert.assertEquals(242, locate);

        ByteBufUtils.release(payload);
        segment.release();
    }

}
