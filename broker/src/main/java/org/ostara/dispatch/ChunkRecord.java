package org.ostara.dispatch;

import io.netty.buffer.ByteBuf;
import org.ostara.ledger.Offset;

public record ChunkRecord(int count, ByteBuf data) {

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

        return message.getInt(4);
    }

    public long getEndIndex() {
        int bytes = data.readableBytes();
        int length = data.getInt(bytes - 4);
        ByteBuf message = data.slice(bytes - 4 - length, length);

        return message.getLong(8);
    }


    public Offset getStartOffset() {
        int location = data.readerIndex();
        int epoch = data.getInt(location + 8);
        long index = data.getLong(location + 12);

        return new Offset(epoch, index);
    }

    public int getStartEpoch() {
        int length = data.getInt(data.readerIndex());
        ByteBuf message = data.slice(4, length);
        return message.getInt(4);
    }

    public long getStartIndex() {
        int length = data.getInt(data.readerIndex());
        ByteBuf message = data.slice(4, length);

        return message.getLong(8);
    }

}
