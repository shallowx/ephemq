package org.shallow.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.Recycler;
import io.netty.util.ReferenceCounted;
import org.shallow.Type;

import javax.annotation.concurrent.Immutable;

import static org.shallow.util.ByteBufUtil.*;

@Immutable
public final class MessagePacket extends AbstractReferenceCounted {
    public static final byte MAGIC_NUMBER = (byte) 0x2c;
    public static final byte HEADER_LENGTH = 12;
    public static final int MAX_FRAME_LENGTH = 4194316;
    public static final int MAX_BODY_LENGTH = MAX_FRAME_LENGTH - HEADER_LENGTH;

    private static final Recycler<MessagePacket> RECYCLER = new Recycler<>() {
        @Override
        protected MessagePacket newObject(Handle<MessagePacket> handle) {
            return new MessagePacket(handle);
        }
    };

    private short version;
    private byte type;
    private int answer;
    private byte command;
    private ByteBuf body;
    private final Recycler.Handle<MessagePacket> handle;

    public static MessagePacket newPacket(int answer, byte command, ByteBuf body) {
       return newPacket((short) -1, (byte) Type.HEARTBEAT.sequence(), answer, command, body);
    }

    public static MessagePacket newPacket(short version, int answer, byte command, ByteBuf body) {
        return newPacket(version, (byte) Type.HEARTBEAT.sequence(), answer, command, body);
    }

    public static MessagePacket newPacket(short version, byte type, int answer, byte command, ByteBuf body) {
        final MessagePacket packet = RECYCLER.get();
        packet.setRefCnt(1);
        packet.version = version;
        packet.type = type;
        packet.answer = answer;
        packet.command = command;
        packet.body = defaultIfNull(body, Unpooled.EMPTY_BUFFER);

        return packet;
    }

    private MessagePacket(Recycler.Handle<MessagePacket> handle) {
        this.handle = handle;
    }

    public int answer() {
        return answer;
    }

    public short version() {
        return version;
    }

    public byte type() {
        return type;
    }

    public ByteBuf body() {
        return body;
    }

    public byte command() {
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
        return "MessagePacket{" +
                "version=" + version +
                ", type=" + type +
                ", answer=" + answer +
                ", command=" + command +
                ", handle=" + handle +
                '}';
    }
}
