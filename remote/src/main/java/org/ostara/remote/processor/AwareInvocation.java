package org.ostara.remote.processor;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.Recycler;
import io.netty.util.ReferenceCounted;
import org.ostara.remote.invoke.InvokeAnswer;

import javax.annotation.concurrent.Immutable;

import static org.ostara.common.util.ObjectUtils.checkPositive;
import static org.ostara.remote.util.ByteBufUtils.defaultIfNull;

@Immutable
public final class AwareInvocation extends AbstractReferenceCounted {

    private static final Recycler<AwareInvocation> RECYCLER = new Recycler<>() {
        @Override
        protected AwareInvocation newObject(Handle<AwareInvocation> handle) {
            return new AwareInvocation(handle);
        }
    };
    private final Recycler.Handle<AwareInvocation> handle;
    private int command;
    private ByteBuf data;
    private long expired;
    private InvokeAnswer<ByteBuf> answer;

    public AwareInvocation(Recycler.Handle<AwareInvocation> handle) {
        this.handle = handle;
    }

    public static AwareInvocation newInvocation(int command, ByteBuf data) {
        return newInvocation(command, data, 0, null);
    }

    public static AwareInvocation newInvocation(int command, ByteBuf data, long expires, InvokeAnswer<ByteBuf> answer) {
        checkPositive(command, "Command");

        final AwareInvocation invocation = RECYCLER.get();
        invocation.setRefCnt(1);
        invocation.command = command;
        invocation.answer = answer;
        invocation.expired = expires;
        invocation.data = defaultIfNull(data, Unpooled.EMPTY_BUFFER);

        return invocation;
    }

    public int command() {
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
