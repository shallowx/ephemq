package org.leopard.remote.util;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;

import javax.naming.OperationNotSupportedException;
import java.io.IOException;

import static org.leopard.remote.util.ByteBufUtils.release;

public class ProtoBufUtils {

    private ProtoBufUtils() throws OperationNotSupportedException {
        // Unused
        throw new OperationNotSupportedException();
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
            release(buf);
            throw t;
        }
    }

    public static int protoLength(MessageLite lite) {
        return lite.getSerializedSize() + 4;
    }
}
