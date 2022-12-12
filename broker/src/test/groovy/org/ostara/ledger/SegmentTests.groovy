package org.ostara.ledger

import io.netty.buffer.PooledByteBufAllocator
import org.ostara.remote.util.ByteBufUtils
import spock.lang.Specification

import java.nio.charset.StandardCharsets

class SegmentTests extends Specification {

    def topic = "test-topic";
    def queue = "test-queue";
    def payload = ByteBufUtils.byte2Buf("testWriteBuf".getBytes(StandardCharsets.UTF_8));

    def "Write message buf to direct memory."() {
        given:
        def bytes = topic.length() + queue.length() + 26 + payload.readableBytes()
        def segment = new Segment(0, PooledByteBufAllocator.DEFAULT.directBuffer(bytes, bytes), new Offset(-1, 0))
        segment.write(topic, queue, (short) 0, payload, new Offset(-1, 1));

        cleanup:
        ByteBufUtils.release(payload);
        segment.release();
    }

    def "read message buf from direct memory."() {
        given:
        def bytes = topic.length() + queue.length() + 26 + payload.readableBytes()
        def segment = new Segment(0, PooledByteBufAllocator.DEFAULT.directBuffer(bytes, bytes), new Offset(-1, 0))
        segment.write(topic, queue, (short) 0, payload, new Offset(-1, 1))

        def buf = segment.read(segment.headLocation())

        def theVersion = buf.readShort()

        def topicLength = buf.readInt()
        def topicBuf = buf.retainedSlice(buf.readerIndex(), topicLength)
        def readTopic = ByteBufUtils.buf2String(topicBuf, topicLength)

        expect:
        readTopic.equals(topic)
        theVersion == 0

        cleanup:
        ByteBufUtils.release(topicBuf)
        ByteBufUtils.release(payload)

        segment.release()
    }

    def "read completed message buf from direct memory."() {
        given:
        def bytes = topic.length() + queue.length() + 26 + payload.readableBytes()
        def segment = new Segment(0, PooledByteBufAllocator.DEFAULT.directBuffer(bytes, bytes), new Offset(-1, 0))
        segment.write(topic, queue, (short) 0, payload, new Offset(-1, 1))

        def buf = segment.readCompleted(segment.headLocation())

        buf.skipBytes(4)
        def theVersion = buf.readShort()

        def topicLength = buf.readInt()
        def topicBuf = buf.retainedSlice(buf.readerIndex(), topicLength)
        def readTopic = ByteBufUtils.buf2String(topicBuf, topicLength)


        expect:
        readTopic.equals(this.topic)
        theVersion == 0

        cleanup:
        ByteBufUtils.release(topicBuf)
        ByteBufUtils.release(payload)

        segment.release();
    }

    def "locate message from direct memory."() {
        given:
        def bytes = topic.length() + queue.length() + 26 + payload.readableBytes()
        def segment = new Segment(0, PooledByteBufAllocator.DEFAULT.directBuffer(bytes * 10, bytes * 10), new Offset(-1, 0))

        for (int i = 0; i < 10; i++) {
            payload.retain()
            segment.write(topic, queue, (short) 0, payload, new Offset(-1, i + 1))
        }

        def locate = segment.locate(new Offset(-1, 5))

        expect:
        locate == 242

        cleanup:
        ByteBufUtils.release(payload);
        segment.release();
    }
}
