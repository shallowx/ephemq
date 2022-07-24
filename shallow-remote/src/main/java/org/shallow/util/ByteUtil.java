package org.shallow.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.shallow.ObjectUtil;

import javax.naming.OperationNotSupportedException;
import java.nio.charset.StandardCharsets;

public final class ByteUtil {

    private ByteUtil() throws OperationNotSupportedException {
        // Unused
        throw new OperationNotSupportedException();
    }

    public static String buf2String(ByteBuf buf, int maxLength) {
        if (ObjectUtil.isNull(buf)) {
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

    public static int bufLength(ByteBuf buf) {
        return buf == null ? 0 : buf.readableBytes();
    }

    public static ByteBuf string2Buf(String data) {
        return ObjectUtil.isNull(data) ? null : Unpooled.copiedBuffer(data, StandardCharsets.UTF_8);
    }

    public static ByteBuf retainBuf(ByteBuf buf) {
        return buf == null ? null : buf.retain();
    }

    public static <T> T defaultIfNull(T t, T defaultValue) {
        return ObjectUtil.isNotNull(t) ? t : defaultValue;
    }

    public static void release(ByteBuf buf) {
       if (ObjectUtil.isNotNull(buf)) {
           buf.release();
       }
    }
}
