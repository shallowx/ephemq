package org.meteor.common.util;


import io.netty.buffer.ByteBuf;
import org.meteor.common.message.Offset;

/**
 * Utility class for handling messages contained in a {@link ByteBuf}.
 */
public class MessageUtil {
    /**
     * Extracts an {@link Offset} object from the given ByteBuf.
     * The offset is determined based on values retrieved from specific positions
     * within the buffer.
     *
     * @param buf the ByteBuf from which to extract the offset
     * @return an {@link Offset} object containing the extracted epoch and index values
     */
    public static Offset getOffset(ByteBuf buf) {
        int location = buf.readerIndex();
        int epoch = buf.getInt(location + 4);
        long index = buf.getLong(location + 8);

        return new Offset(epoch, index);
    }

    /**
     * Retrieves an integer marker from the start of the specified {@code ByteBuf}.
     *
     * @param buf the buffer from which to retrieve the marker
     * @return the integer marker found at the beginning of the buffer
     */
    public static int getMarker(ByteBuf buf) {
        return buf.getInt(0);
    }

    /**
     * Retrieves the epoch value from the specified ByteBuf.
     *
     * @param buf the ByteBuf from which to extract the epoch value
     * @return the epoch value as an integer
     */
    public static int getEpoch(ByteBuf buf) {
        return buf.getInt(4);
    }

    /**
     * Retrieves the index value from the provided {@link ByteBuf} buffer.
     *
     * @param buf the {@link ByteBuf} instance containing the index data.
     * @return the index value extracted from the buffer.
     */
    public static int getIndex(ByteBuf buf) {
        return buf.getInt(8);
    }

    /**
     * Extracts and returns a portion of the provided {@code ByteBuf} that represents the metadata section.
     *
     * @param buf the source {@code ByteBuf} from which to extract the metadata.
     * @return a new {@code ByteBuf} instance containing the metadata extracted from the provided buffer.
     */
    public static ByteBuf getMeta(ByteBuf buf) {
        int length = buf.getInt(16);
        return buf.retainedSlice(20, length);
    }

    /**
     * Extracts the body portion from the provided {@code ByteBuf} message buffer.
     * It reads the metadata length at a fixed offset, and then slices the buffer
     * to exclude the header and the metadata.
     *
     * @param buf the {@code ByteBuf} from which to extract the body. Must not be null.
     * @return a new {@code ByteBuf} representing the body part of the message.
     */
    public static ByteBuf getBody(ByteBuf buf) {
        int length = buf.getInt(16);
        return buf.slice(20 + length, buf.readableBytes() - 20 - length);
    }

    /**
     * Extracts the payload from the given ByteBuf starting at the 16th byte.
     *
     * @param buf the ByteBuf containing the data from which the payload will be extracted
     * @return a sliced ByteBuf containing only the payload portion of the original ByteBuf
     */
    public static ByteBuf getPayload(ByteBuf buf) {
        return buf.slice(16, buf.readableBytes() - 16);
    }

    /**
     * Determines whether two given {@code Offset} instances are continuous. Two offsets are considered
     * continuous if they belong to the same epoch and the second offset's index
     * is exactly one more than the first offset's index, or if they belong to
     * consecutive epochs and the second offset's index is 1.
     *
     * @param baseOffset the base {@code Offset} to be compared
     * @param nextOffset the next {@code Offset} to be checked for continuity
     * @return {@code true} if the offsets are continuous, otherwise {@code false}
     */
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
