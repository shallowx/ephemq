package org.meteor.ledger;

import io.netty.buffer.ByteBuf;
import org.meteor.common.message.Offset;
import org.meteor.common.internal.MessageUtil;

public record ChunkRecord(int count, ByteBuf data) {
    public Offset getStartOffset() {
        int location = data.readerIndex();
        int epoch = data.getInt(location + 8);
        long index = data.getLong(location + 12);
        return new Offset(epoch, index);
    }

    public int getStartEpoch() {
        int length = data.getInt(data.readableBytes());
        ByteBuf message = data.slice(4, length);
        return MessageUtil.getEpoch(message);
    }

    public long getStartIndex() {
        int length = data.getInt(data.readableBytes());
        ByteBuf message = data.slice(4, length);
        return MessageUtil.getEpoch(message);
    }

    public Offset getEndOffset() {
        int location = data.writerIndex();
        int length = data.getInt(location - 4);
        int epoch = data.getInt(location - length);
        long index = data.getLong(location - length + 4);
        return new Offset(epoch, index);
    }

    public int getEndEpoch() {
        int bytes = data.readableBytes();
        int length = data.getInt(bytes - 4);
        ByteBuf message = data.slice(bytes - 4 - length, length);
        return MessageUtil.getEpoch(message);
    }

    public long getEndIndex() {
        int bytes = data.readableBytes();
        int length = data.getInt(bytes - 4);
        ByteBuf message = data.slice(bytes - 4 - length, length);
        return MessageUtil.getIndex(message);
    }
}
