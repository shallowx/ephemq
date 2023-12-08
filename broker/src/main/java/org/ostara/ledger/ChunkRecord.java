package org.ostara.ledger;

import io.netty.buffer.ByteBuf;
import org.ostara.common.Offset;
import org.ostara.common.util.MessageUtils;

public class ChunkRecord {
    private final int count;
    private final ByteBuf data;

    public ChunkRecord(int count, ByteBuf data) {
        this.count = count;
        this.data = data;
    }

    public ByteBuf data() {
        return data;
    }
    public int count() {
        return count;
    }

    public Offset getStartOffset() {
        int location = data.readerIndex();
        int epoch = data.getInt( location + 8);
        long index = data.getLong( location + 12);
        return new Offset(epoch, index);
    }
    public int getStartEpoch() {
        int length = data.getInt(data.readableBytes());
        ByteBuf message = data.slice( 4 , length);
        return MessageUtils.getEpoch(message);
    }

    public long getStartIndex() {
        int length = data.getInt(data.readableBytes());
        ByteBuf message = data.slice( 4 , length);
        return MessageUtils.getEpoch(message);
    }

    public Offset getEndOffset() {
        int location = data.writerIndex();
        int length = data.getInt( location - 4);
        int epoch = data.getInt( location - length);
        long index = data.getLong( location - location + 4);
        return new Offset(epoch, index);
    }
    public int getEndEpoch() {
        int bytes = data.readableBytes();
        int length = data.getInt(bytes - 4);
        ByteBuf message = data.slice(bytes - 4 - length, length);
        return MessageUtils.getEpoch(message);
    }

    public long getEndIndex() {
        int bytes = data.readableBytes();
        int length = data.getInt(bytes - 4);
        ByteBuf message = data.slice(bytes - 4 - length, length);
        return MessageUtils.getIndex(message);
    }
}
