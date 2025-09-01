package org.ephemq.ledger;

import io.netty.buffer.ByteBuf;
import org.ephemq.common.message.Offset;
import org.ephemq.common.util.MessageUtil;

/**
 * Represents a chunk record that contains an integer count and a ByteBuf data.
 * Provides various methods to extract offsets and epoch information from the ByteBuf.
 */
public record ChunkRecord(int count, ByteBuf data) {
    /**
     * Retrieves the start offset of this chunk record.
     *
     * @return The start offset represented by an Offset object, which contains
     * the epoch and index derived from the current reader position in the data buffer.
     */
    public Offset getStartOffset() {
        int location = data.readerIndex();
        int epoch = data.getInt(location + 8);
        long index = data.getLong(location + 12);
        return new Offset(epoch, index);
    }

    /**
     * Retrieves the starting epoch from the data buffer.
     *
     * @return the starting epoch as an integer
     */
    public int getStartEpoch() {
        int length = data.getInt(data.readableBytes());
        ByteBuf message = data.slice(4, length);
        return MessageUtil.getEpoch(message);
    }

    /**
     * Retrieves the start index from the ByteBuf data.
     *
     * @return the start index as a long value.
     */
    public long getStartIndex() {
        int length = data.getInt(data.readableBytes());
        ByteBuf message = data.slice(4, length);
        return MessageUtil.getEpoch(message);
    }

    /**
     * Retrieves the end offset of this chunk record.
     *
     * @return an Offset object representing the end location, determined by the epoch and index.
     */
    public Offset getEndOffset() {
        int location = data.writerIndex();
        int length = data.getInt(location - 4);
        int epoch = data.getInt(location - length);
        long index = data.getLong(location - length + 4);
        return new Offset(epoch, index);
    }

    /**
     * Retrieves the epoch value associated with the end of the chunk record.
     *
     * @return the epoch value from the end of the chunk's data.
     */
    public int getEndEpoch() {
        int bytes = data.readableBytes();
        int length = data.getInt(bytes - 4);
        ByteBuf message = data.slice(bytes - 4 - length, length);
        return MessageUtil.getEpoch(message);
    }

    /**
     * Computes and returns the end index from the ByteBuf data.
     *
     * @return the end index extracted from the ByteBuf data.
     */
    public long getEndIndex() {
        int bytes = data.readableBytes();
        int length = data.getInt(bytes - 4);
        ByteBuf message = data.slice(bytes - 4 - length, length);
        return MessageUtil.getIndex(message);
    }
}
