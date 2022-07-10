package org.shallow.util;

import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

public final class ByteUtil {
    private ByteUtil() {}

    public static String byteToString(ByteBuf buf, int maxLength) {
        if (buf == null) {
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

    public static <T> T defaultIfNull(T t, T defaultValue) {
        return t != null ? t : defaultValue;
    }

    public static boolean release(ByteBuf buf) {
        if (buf == null) {
            return Boolean.FALSE;
        }
        return buf.release();
    }
}
