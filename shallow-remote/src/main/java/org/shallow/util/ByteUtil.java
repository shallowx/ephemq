package org.shallow.util;

import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

import static org.shallow.ObjectUtil.isNotNull;
import static org.shallow.ObjectUtil.isNull;

public final class ByteUtil {
    private ByteUtil() {}

    public static String bufToString(ByteBuf buf, int maxLength) {
        if (isNull(buf)) {
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
        return isNotNull(t) ? t : defaultValue;
    }

    public static boolean release(ByteBuf buf) {
        if (isNull(buf)) {
            return Boolean.FALSE;
        }
        return buf.release();
    }
}
