package org.shallow.codec;

import io.netty.buffer.ByteBuf;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.Recycler;

import static org.shallow.util.ByteUtil.byteToString;

public final class ShallowPacket extends AbstractReferenceCounted {
    public static final byte MAGIC_NUMBER = (byte) 0x2c;
    public static final byte HEADER_LENGTH = 10;
    public static final int MAX_FRAME_LENGTH = 4194314;
    public static final int MAX_BODY_LENGTH = MAX_FRAME_LENGTH - HEADER_LENGTH;

    private static final Recycler<ShallowPacket> RECYCLER = new Recycler<ShallowPacket>() {
        @Override
        protected ShallowPacket newObject(Handle<ShallowPacket> handle) {
            return new ShallowPacket(handle);
        }
    };

    private short version;
    private byte state;
    private int answer;
    private byte serialization;
    private ByteBuf body;
    private final Recycler.Handle<ShallowPacket> handle;

    private ShallowPacket(Recycler.Handle<ShallowPacket> handle) {
        this.handle = handle;
    }

    public int answer() {
        return answer;
    }

    public int version() {
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
                ", body=" + byteToString(body, -1) +
                '}';
    }
}
