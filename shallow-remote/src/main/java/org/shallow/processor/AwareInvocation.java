package org.shallow.processor;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.Recycler;
import io.netty.util.ReferenceCounted;
import org.shallow.invoke.InvokeAnswer;

import static org.shallow.util.ObjectUtil.checkPositive;
import static org.shallow.util.ObjectUtil.isNotNull;
import static org.shallow.util.ByteUtil.defaultIfNull;

public class AwareInvocation extends AbstractReferenceCounted {

    private static final Recycler<AwareInvocation> RECYCLER = new Recycler<AwareInvocation>() {
        @Override
        protected AwareInvocation newObject(Handle<AwareInvocation> handle) {
            return new AwareInvocation(handle);
        }
    };

    private byte command;
    private ByteBuf data;
    private long expired;
    private InvokeAnswer<ByteBuf> answer;

    private final Recycler.Handle<AwareInvocation> handle;

    public AwareInvocation(Recycler.Handle<AwareInvocation> handle) {
        this.handle = handle;
    }

    public static  AwareInvocation newInvocation(byte command, ByteBuf data) {
       return newInvocation(command, data, 0, null);
    }

    public static  AwareInvocation newInvocation(byte command, ByteBuf data, long expires, InvokeAnswer<ByteBuf> answer) {
        checkPositive(command, "Command");

        final AwareInvocation invocation = RECYCLER.get();
        invocation.setRefCnt(1);
        invocation.command = command;
        invocation.answer = answer;
        invocation.expired = expires;
        invocation.data = defaultIfNull(data, Unpooled.EMPTY_BUFFER);

        return invocation;
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
        if (isNotNull(data)) {
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
        if (isNotNull(data)) {
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
