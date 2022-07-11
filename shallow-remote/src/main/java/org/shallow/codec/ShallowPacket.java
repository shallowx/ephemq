package org.shallow.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.Recycler;
import io.netty.util.ReferenceCounted;

import static org.shallow.util.ByteUtils.byteToString;
import static org.shallow.util.ByteUtils.defaultIfNull;

public final class ShallowPacket extends AbstractReferenceCounted {
    public static final byte MAGIC_NUMBER = (byte) 0x2c;
    public static final byte HEADER_LENGTH = 13;
    public static final int MAX_FRAME_LENGTH = 4194317;
    public static final int MAX_BODY_LENGTH = MAX_FRAME_LENGTH - HEADER_LENGTH;

    private static final Recycler<ShallowPacket> RECYCLER = new Recycler<>() {
        @Override
        protected ShallowPacket newObject(Handle<ShallowPacket> handle) {
            return new ShallowPacket(handle);
        }
    };

    private short version;
    private byte state;
    private int answer;
    private byte serialization;
    private byte command;
    private ByteBuf body;
    private final Recycler.Handle<ShallowPacket> handle;

    public static ShallowPacket newPacket(short version, byte state, int answer, byte serialization, byte command, ByteBuf body) {
        final ShallowPacket packet = RECYCLER.get();
        packet.setRefCnt(1);
        packet.version = version;
        packet.state = state;
        packet.answer = answer;
        packet.serialization = serialization;
        packet.command = command;
        packet.body = defaultIfNull(body, Unpooled.EMPTY_BUFFER);

        return packet;
    }

    private ShallowPacket(Recycler.Handle<ShallowPacket> handle) {
        this.handle = handle;
    }

    public int answer() {
        return answer;
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
        if (body != null) {
            body.readByte();
            body = null;
        }
        handle.recycle(this);
    }

    @Override
    public ShallowPacket retain() {
        super.retain();
        return this;
    }

    @Override
    public ShallowPacket retain(int increment) {
        super.retain(increment);
        return this;
    }

    @Override
    public ReferenceCounted touch() {
        super.touch();
        return this;
    }

    @Override
    public ShallowPacket touch(Object hint) {
        if (body != null) {
            body.touch(hint);
        }
        return this;
    }

    @Override
    public String toString() {
        return "ShallowPacket{" +
                "answer=" + answer +
                ", version=" + version +
                ", serialization=" + serialization +
                ", state=" + state +
                ", command=" + command +
                ", body=" + byteToString(body, -1) +
                '}';
    }
}
