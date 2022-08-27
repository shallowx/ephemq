package org.shallow.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import javax.naming.OperationNotSupportedException;
import java.nio.charset.StandardCharsets;

import static org.shallow.util.ObjectUtil.isNotNull;
import static org.shallow.util.ObjectUtil.isNull;

public final class ByteBufUtil {

    private ByteBufUtil() throws OperationNotSupportedException {
        // Unused
        throw new OperationNotSupportedException();
    }

    public static String buf2String(ByteBuf buf, int maxLength) {
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

    public static ByteBuf byte2Buf(byte[] bytes) {
        if (isNull(bytes) || bytes.length == 0) {
            return null;
        }
       return Unpooled.copiedBuffer(bytes);
    }

    public static byte[] buf2Bytes(ByteBuf buf) {
        if (isNull(buf)) {
            return null;
        }
        int length = buf.readableBytes();
        byte[] bytes = new byte[length];
        buf.readBytes(bytes);

        return bytes;
    }

    public static int bufLength(ByteBuf buf) {
        return isNull(buf) ? 0 : buf.readableBytes();
    }

    public static ByteBuf string2Buf(String data) {
        return isNull(data) ? null : Unpooled.copiedBuffer(data, StandardCharsets.UTF_8);
    }

    public static ByteBuf retainBuf(ByteBuf buf) {
        return isNull(buf) ? null : buf.retain();
    }

    public static <T> T defaultIfNull(T t, T defaultValue) {
        return isNotNull(t) ? t : defaultValue;
    }

    public static void release(ByteBuf buf) {
       if (isNotNull(buf)) {
           buf.release();
       }
    }
}
