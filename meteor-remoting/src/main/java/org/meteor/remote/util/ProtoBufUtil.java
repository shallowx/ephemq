package org.meteor.remote.util;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import io.netty.buffer.*;

import java.io.IOException;

/**
 * Utility class for handling operations related to ProtoBuf serialization and deserialization
 * using Netty's ByteBuf.
 */
public class ProtoBufUtil {
    /**
     * Constructs a ProtoBufUtil instance.
     * <p>
     * This is a utility class for handling operations related to ProtoBuf
     * serialization and deserialization using Netty's ByteBuf and should not be instantiated.
     * The constructor is private to prevent instantiation and throws an AssertionError if called.
     */
    private ProtoBufUtil() {
        throw new AssertionError("No org.meteor.remote.util.ProtoBufUtil instance for you");
    }

    /**
     * Reads a protocol buffer message from the provided ByteBuf using the specified parser.
     *
     * @param buf the ByteBuf containing the serialized protocol buffer message
     * @param parser the parser used to deserialize the protocol buffer message
     * @param <T> the type of the protocol buffer message
     * @return the deserialized protocol buffer message
     * @throws InvalidProtocolBufferException if the input is not a valid protocol buffer message or if an I/O error occurs while reading from the buffer
     */
    public static <T> T readProto(ByteBuf buf, Parser<T> parser) throws InvalidProtocolBufferException {
        int length = buf.readInt();
        return parser.parseFrom(new ByteBufInputStream(buf, length));
    }

    /**
     * Serializes and writes a Protocol Buffers message to the given ByteBuf.
     *
     * @param buf  the ByteBuf to write the serialized message into
     * @param lite the Protocol Buffers message to be serialized and written
     * @throws IOException if an I/O error occurs during serialization
     */
    public static void writeProto(ByteBuf buf, MessageLite lite) throws IOException {
        buf.writeInt(lite.getSerializedSize());
        lite.writeTo(new ByteBufOutputStream(buf));
    }

    /**
     * Serializes a protocol buffer message into a Netty ByteBuf.
     *
     * @param alloc The ByteBufAllocator used to allocate the ByteBuf.
     * @param lite The protocol buffer message to be serialized.
     * @return A ByteBuf containing the serialized protocol buffer message.
     * @throws IOException If an I/O error occurs during serialization.
     */
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

    /**
     * Creates a {@link ByteBuf} that contains a serialized protocol buffer message
     * followed by an optional payload.
     *
     * @param allocator the {@link ByteBufAllocator} to use for allocating the buffer
     * @param proto the {@link MessageLite} protocol buffer message to serialize
     * @param payload the optional payload to append to the buffer; may be null
     * @return a {@link ByteBuf} containing the serialized protocol buffer followed by the payload
     * @throws Exception if an error occurs during serialization or buffer allocation
     */
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

    /**
     * Calculates the length of a serialized Protocol Buffers message including an additional buffer.
     *
     * @param lite the Protocol Buffers message to be serialized
     * @return the length of the serialized message including an additional 4-byte buffer
     */
    public static int protoLength(MessageLite lite) {
        return lite.getSerializedSize() + 4;
    }
}
