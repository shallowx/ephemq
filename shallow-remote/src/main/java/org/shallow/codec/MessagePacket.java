package org.shallow.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.Recycler;
import io.netty.util.ReferenceCounted;

import static org.shallow.ObjectUtil.isNotNull;
import static org.shallow.util.ByteUtil.bufToString;
import static org.shallow.util.ByteUtil.defaultIfNull;

public final class MessagePacket extends AbstractReferenceCounted {
    public static final byte MAGIC_NUMBER = (byte) 0x2c;
    public static final byte HEADER_LENGTH = 13;
    public static final int MAX_FRAME_LENGTH = 4194317;
    public static final int MAX_BODY_LENGTH = MAX_FRAME_LENGTH - HEADER_LENGTH;

    private static final Recycler<MessagePacket> RECYCLER = new Recycler<>() {
        @Override
        protected MessagePacket newObject(Handle<MessagePacket> handle) {
            return new MessagePacket(handle);
        }
    };

    private short version;
    private byte state; // req: -1 ; resp:response code
    private int rejoin; // opaque
    private byte serialization; // default: -1
    private byte command;
    private ByteBuf body;
    private final Recycler.Handle<MessagePacket> handle;

    public static MessagePacket newPacket(int rejoin, byte command, ByteBuf body) {
       return newPacket((short) -1, (byte) -1, rejoin, (byte) -1, command, body);
    }


    public static MessagePacket newPacket(short version, byte state, int rejoin, byte serialization, byte command, ByteBuf body) {
        final MessagePacket packet = RECYCLER.get();
        packet.setRefCnt(1);
        packet.version = version;
        packet.state = state;
        packet.rejoin = rejoin;
        packet.serialization = serialization;
        packet.command = command;
        packet.body = defaultIfNull(body, Unpooled.EMPTY_BUFFER);

        return packet;
    }

    private MessagePacket(Recycler.Handle<MessagePacket> handle) {
        this.handle = handle;
    }

    public int rejoin() {
        return rejoin;
    }

    public short version() {
        return version;
    }

    public byte serialization() {
        return serialization;
    }

    public byte state() {
        return state;
    }

    public ByteBuf body() {
        return body;
    }

    public byte command() {
        return command;
    }

    @Override
    protected void deallocate() {
        if (isNotNull(body)) {
            body.readByte();
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
        if (isNotNull(body)) {
            body.touch(hint);
        }
        return this;
    }

    @Override
    public String toString() {
        return "ShallowPacket{" +
                "rejoin=" + rejoin +
                ", version=" + version +
                ", serialization=" + serialization +
                ", state=" + state +
                ", command=" + command +
                ", body=" + bufToString(body, -1) +
                '}';
    }
}
