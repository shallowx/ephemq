package org.meteor.common.util;


import io.netty.buffer.ByteBuf;
import org.meteor.common.message.Offset;

public class MessageUtil {
    public static Offset getOffset(ByteBuf buf) {
        int location = buf.readerIndex();
        int epoch = buf.getInt(location + 4);
        long index = buf.getLong(location + 8);

        return new Offset(epoch, index);
    }

    public static int getMarker(ByteBuf buf) {
        return buf.getInt(0);
    }

    public static int getEpoch(ByteBuf buf) {
        return buf.getInt(4);
    }

    public static int getIndex(ByteBuf buf) {
        return buf.getInt(8);
    }

    public static ByteBuf getMeta(ByteBuf buf) {
        int length = buf.getInt(16);
        return buf.retainedSlice(20, length);
    }

    public static ByteBuf getBody(ByteBuf buf) {
        int length = buf.getInt(16);
        return buf.slice(20 + length, buf.readableBytes() - 20 - length);
    }

    public static ByteBuf getPayload(ByteBuf buf) {
        return buf.slice(16, buf.readableBytes() - 16);
    }

    public static boolean isContinuous(Offset baseOffset, Offset nextOffset) {
        if (baseOffset == null || nextOffset == null) {
            return true;
        }

        int baseEpoch = baseOffset.getEpoch();
        if (baseEpoch < 0) {
            return true;
        }

        int nextEpoch = nextOffset.getEpoch();
        if (baseEpoch == nextEpoch) {
            long baseIndex = baseOffset.getIndex();
            return baseIndex <= 0 || baseIndex + 1 == nextOffset.getIndex();
        } else if (baseEpoch < nextEpoch) {
            long baseIndex = baseOffset.getIndex();
            return baseIndex <= 0 || nextOffset.getIndex() == 1;
        } else {
            return false;
        }
    }
}
