package org.meteor.remote.util;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import io.netty.buffer.*;

import java.io.IOException;

public class ProtoBufUtil {
    private ProtoBufUtil() {
        throw new AssertionError("No org.meteor.remote.util.ProtoBufUtil instance for you");
    }

    public static <T> T readProto(ByteBuf buf, Parser<T> parser) throws InvalidProtocolBufferException {
        int length = buf.readInt();
        return parser.parseFrom(new ByteBufInputStream(buf, length));
    }

    public static void writeProto(ByteBuf buf, MessageLite lite) throws IOException {
        buf.writeInt(lite.getSerializedSize());
        lite.writeTo(new ByteBufOutputStream(buf));
    }

    public static ByteBuf proto2Buf(ByteBufAllocator alloc, MessageLite lite) throws IOException {
        ByteBuf buf = null;
        try {
            buf = alloc.ioBuffer(protoLength(lite));
            writeProto(buf, lite);

            return buf;
        } catch (Throwable t) {
            ByteBufUtil.release(buf);
            throw t;
        }
    }

    public static ByteBuf protoPayloadBuf(ByteBufAllocator allocator, MessageLite proto, ByteBuf payload) throws Exception {
        ByteBuf buf = null;
        try {
            boolean isDirect = payload != null && payload.isDirect();
            buf = allocator.ioBuffer(protoLength(proto) + (isDirect ? 0 : ByteBufUtil.bufLength(payload)));
            writeProto(buf, proto);
            if (isDirect) {
                buf = Unpooled.wrappedUnmodifiableBuffer(buf, payload.retainedSlice());
            } else if (payload != null) {
                buf.writeBytes(payload, payload.readerIndex(), payload.readableBytes());
            }
            return buf;
        } catch (Throwable t) {
            if (buf != null) {
                buf.release();
            }
            throw t;
        }
    }

    public static int protoLength(MessageLite lite) {
        return lite.getSerializedSize() + 4;
    }
}
