package org.meteor.remote.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.charset.StandardCharsets;

/**
 * Utility class for operations related to Netty's ByteBuf.
 * Contains methods to convert between ByteBuf and other data types,
 * as well as utility methods for handling ByteBuf instances.
 */
public final class ByteBufUtil {
    /**
     * Constructs a ByteBufUtil instance.
     * <p>
     * This is a utility class and should not be instantiated.
     * The constructor is private to prevent instantiation and
     * throws an AssertionError if called.
     */
    private ByteBufUtil() {
        throw new AssertionError("No org.meteor.remote.util.ByteBufUtil instance for you");
    }

    /**
     * Converts a ByteBuf to a String using the specified maximum length.
     *
     * @param buf the ByteBuf to convert to a string
     * @param maxLength the maximum length of the string to be returned; if negative, the entire buffer content is used
     * @return the string representation of the ByteBuf or null if the input buffer is null or an error occurs
     */
    public static String buf2String(ByteBuf buf, int maxLength) {
        if (null == buf) {
            return null;
        }

        int length = buf.readableBytes();
        length = maxLength < 0 ? length : Math.min(maxLength, length);
        try {
            return buf.toString(buf.readerIndex(), length, StandardCharsets.UTF_8);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Converts a byte array to a Netty ByteBuf.
     *
     * @param bytes the byte array to be converted, may be null or empty.
     * @return the resulting ByteBuf or null if the input byte array is null or empty.
     */
    @SuppressWarnings("unused")
    public static ByteBuf byte2Buf(byte[] bytes) {
        if (null == bytes || bytes.length == 0) {
            return null;
        }
        return Unpooled.copiedBuffer(bytes);
    }

    /**
     * Converts a Netty ByteBuf into a byte array.
     *
     * @param buf the ByteBuf to be converted; can be null.
     * @return a byte array containing the data from the ByteBuf; null if the input ByteBuf is null.
     */
    public static byte[] buf2Bytes(ByteBuf buf) {
        if (null == buf) {
            return null;
        }

        int length = buf.readableBytes();
        byte[] bytes = new byte[length];
        buf.readBytes(bytes);

        return bytes;
    }

    /**
     * Calculates the length of the readable bytes in the given ByteBuf.
     *
     * @param buf the ByteBuf for which the readable length is to be calculated
     * @return the number of readable bytes in the ByteBuf, or 0 if the ByteBuf is null
     */
    public static int bufLength(ByteBuf buf) {
        return null == buf ? 0 : buf.readableBytes();
    }

    /**
     * Converts a string into a Netty ByteBuf instance encoded in UTF-8.
     *
     * @param data the string to be converted; can be null.
     * @return a ByteBuf containing the UTF-8 encoded bytes of the string,
     *         or null if the input string is null.
     */
    public static ByteBuf string2Buf(String data) {
        return null == data ? null : Unpooled.copiedBuffer(data, StandardCharsets.UTF_8);
    }

    /**
     * Retains the specified ByteBuf, incrementing its reference count.
     * This ensures that the buffer's resources are not released until the reference count reaches zero.
     *
     * @param buf the ByteBuf to retain; may be null
     * @return the retained ByteBuf, or null if the input was null
     */
    public static ByteBuf retainBuf(ByteBuf buf) {
        return null == buf ? null : buf.retain();
    }

    /**
     * Returns the given object if it is non-null, otherwise returns a default value.
     *
     * @param t the object to check for nullity
     * @param defaultValue the default value to return if the object is null
     * @return the object if it is non-null, otherwise the default value
     */
    public static <T> T defaultIfNull(T t, T defaultValue) {
        return null != t ? t : defaultValue;
    }

    /**
     * Releases the specified {@link ByteBuf} if it is not null.
     *
     * @param buf the {@link ByteBuf} to be released, may be null
     */
    public static void release(ByteBuf buf) {
        if (null != buf) {
            buf.release();
        }
    }
}
