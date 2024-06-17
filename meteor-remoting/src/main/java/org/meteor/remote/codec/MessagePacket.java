package org.meteor.remote.codec;

import static org.meteor.remote.util.ByteBufUtil.defaultIfNull;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.Recycler;
import io.netty.util.ReferenceCounted;
import javax.annotation.concurrent.Immutable;

@Immutable
public final class MessagePacket extends AbstractReferenceCounted {
    public static final byte MAGIC_NUMBER = (byte) 0x2c;
    public static final byte HEADER_LENGTH = 16;
    public static final int MAX_FRAME_LENGTH = 4194316;
    public static final int MAX_BODY_LENGTH = MAX_FRAME_LENGTH - HEADER_LENGTH;

    private static final Recycler<MessagePacket> RECYCLER = new Recycler<>() {
        @Override
        protected MessagePacket newObject(Handle<MessagePacket> handle) {
            return new MessagePacket(handle);
        }
    };

    private final Recycler.Handle<MessagePacket> handle;
    private long feedback;
    private int command;
    private ByteBuf body;

    private MessagePacket(Recycler.Handle<MessagePacket> handle) {
        this.handle = handle;
    }

    public static MessagePacket newPacket(long feedback, int command, ByteBuf body) {
        final MessagePacket packet = RECYCLER.get();
        packet.setRefCnt(1);
        packet.feedback = feedback;
        packet.command = command;
        packet.body = defaultIfNull(body, Unpooled.EMPTY_BUFFER);

        return packet;
    }


    public long feedback() {
        return feedback;
    }

    public ByteBuf body() {
        return body;
    }

    public int command() {
        return command;
    }

    @Override
    protected void deallocate() {
        if (null != body) {
            body.release();
            body = null;
        }
        handle.recycle(this);
    }

    @Override
    public MessagePacket retain() {
        super.retain();
        return this;
    }

    @Override
    public MessagePacket retain(int increment) {
        super.retain(increment);
        return this;
    }

    @Override
    public ReferenceCounted touch() {
        super.touch();
        return this;
    }

    @Override
    public MessagePacket touch(Object hint) {
        if (null != body) {
            body.touch(hint);
        }
        return this;
    }

    @Override
    public String toString() {
        return "(" +
                ", feedback=" + feedback +
                ", command=" + command +
                ", body=" + body +
                ')';
    }
}
