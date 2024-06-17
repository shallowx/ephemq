package org.meteor.remote.invoke;

import static org.meteor.common.util.ObjectUtil.checkPositive;
import static org.meteor.remote.util.ByteBufUtil.defaultIfNull;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.Recycler;
import io.netty.util.ReferenceCounted;
import javax.annotation.concurrent.Immutable;

@Immutable
public final class WrappedInvocation extends AbstractReferenceCounted {
    private static final Recycler<WrappedInvocation> RECYCLER = new Recycler<>() {
        @Override
        protected WrappedInvocation newObject(Handle<WrappedInvocation> handle) {
            return new WrappedInvocation(handle);
        }
    };
    private final Recycler.Handle<WrappedInvocation> handle;
    private int command;
    private ByteBuf data;
    private long expired;
    private InvokedFeedback<ByteBuf> feedback;

    public WrappedInvocation(Recycler.Handle<WrappedInvocation> handle) {
        this.handle = handle;
    }

    public static WrappedInvocation newInvocation(int command, ByteBuf data) {
        return newInvocation(command, data, 0, null);
    }

    public static WrappedInvocation newInvocation(int command, ByteBuf data, long expires,
                                                  InvokedFeedback<ByteBuf> feedback) {
        checkPositive(command, "Command");

        final WrappedInvocation invocation = RECYCLER.get();
        invocation.setRefCnt(1);
        invocation.command = command;
        invocation.feedback = feedback;
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

    public InvokedFeedback<ByteBuf> feedback() {
        return feedback;
    }

    @Override
    protected void deallocate() {
        if (null != data) {
            data.release();
            data = null;
        }
        feedback = null;
        handle.recycle(this);
    }

    @Override
    public WrappedInvocation retain() {
        super.retain();
        return this;
    }

    @Override
    public ReferenceCounted retain(int increment) {
        super.retain(increment);
        return this;
    }

    @Override
    public WrappedInvocation touch(Object hint) {
        if (null != data) {
            data.touch(hint);
        }
        return this;
    }

    @Override
    public WrappedInvocation touch() {
        super.touch();
        return this;
    }

    @Override
    public String toString() {
        return "(" +
                "handle=" + handle +
                ", command=" + command +
                ", data=" + data +
                ", expired=" + expired +
                ", feedback=" + feedback +
                ')';
    }
}
