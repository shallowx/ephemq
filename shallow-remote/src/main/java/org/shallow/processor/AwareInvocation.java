package org.shallow.processor;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.Recycler;
import io.netty.util.ReferenceCounted;
import org.shallow.invoke.GenericInvokeAnswer;
import org.shallow.invoke.InvokeAnswer;

import javax.annotation.concurrent.Immutable;

import static org.shallow.util.ObjectUtil.checkPositive;
import static org.shallow.util.ByteBufUtil.defaultIfNull;

@Immutable
public class AwareInvocation extends AbstractReferenceCounted {

    private static final Recycler<AwareInvocation> RECYCLER = new Recycler<>() {
        @Override
        protected AwareInvocation newObject(Handle<AwareInvocation> handle) {
            return new AwareInvocation(handle);
        }
    };

    private byte command;
    private ByteBuf data;
    private long expired;
    private byte type;
    private short version;
    private InvokeAnswer<ByteBuf> answer;

    private final Recycler.Handle<AwareInvocation> handle;

    public AwareInvocation(Recycler.Handle<AwareInvocation> handle) {
        this.handle = handle;
    }

    public static  AwareInvocation newInvocation(byte command, short version, ByteBuf data, byte type) {
       return newInvocation(command, version, data, type, 0, null);
    }

    public static  AwareInvocation newInvocation(byte command, short version, ByteBuf data, byte type, long expires, InvokeAnswer<ByteBuf> answer) {
        checkPositive(command, "Command");

        final AwareInvocation invocation = RECYCLER.get();
        invocation.setRefCnt(1);
        invocation.command = command;
        invocation.answer = answer;
        invocation.version = version;
        invocation.type = type;
        invocation.expired = expires;
        invocation.data = defaultIfNull(data, Unpooled.EMPTY_BUFFER);

        return invocation;
    }

    public byte type() {
        return type;
    }

    public short version() {
        return version;
    }

    public byte command() {
        return command;
    }

    public ByteBuf data() {
        return data;
    }

    public long expired() {
        return expired;
    }

    public InvokeAnswer<ByteBuf> answer() {
        return answer;
    }

    @Override
    protected void deallocate() {
        if (null != data) {
            data.release();
            data = null;
        }
        answer = null;
        handle.recycle(this);
    }

    @Override
    public AwareInvocation retain() {
        super.retain();
        return this;
    }

    @Override
    public ReferenceCounted retain(int increment) {
        super.retain(increment);
        return this;
    }

    @Override
    public AwareInvocation touch(Object hint) {
        if (null != data) {
            data.touch(hint);
        }
        return this;
    }

    @Override
    public AwareInvocation touch() {
        super.touch();
        return this;
    }
}
