package org.meteor.client.core;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.EventExecutor;
import java.net.SocketAddress;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nonnull;
import org.meteor.remote.invoke.Callable;
import org.meteor.remote.invoke.GenericInvokedFeedback;
import org.meteor.remote.invoke.InvokedFeedback;
import org.meteor.remote.invoke.WrappedInvocation;
import org.meteor.remote.util.ByteBufUtil;

public class ClientChannel implements MeterBinder {
    private final SocketAddress address;
    private final CommandInvoker invoker;
    protected String id;
    protected Channel channel;
    protected Semaphore semaphore;
    private final int hashCode;

    public ClientChannel(ClientConfig config, Channel channel, SocketAddress address) {
        this.id = channel.id().asLongText();
        this.channel = channel;
        this.address = address;
        this.semaphore = new Semaphore(config.getChannelInvokePermits());
        this.invoker = new CommandInvoker(this);
        this.hashCode = Objects.hashCode(id);
    }


    public void invoke(int code, ByteBuf data, int timeoutMs, Callable<ByteBuf> callback) {
        int length = ByteBufUtil.bufLength(data);
        try {
            long time = System.currentTimeMillis();
            if (semaphore.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS)) {
                try {
                    if (callback == null) {
                        ChannelPromise promise = channel.newPromise().addListener(f -> semaphore.release());
                        channel.writeAndFlush(WrappedInvocation.newInvocation(code, ByteBufUtil.retainBuf(data)),
                                promise);
                    } else {
                        long expires = timeoutMs + time;
                        InvokedFeedback<ByteBuf> feedback = new GenericInvokedFeedback<>((v, c) -> {
                            semaphore.release();
                            callback.onCompleted(v, c);
                        });
                        channel.writeAndFlush(
                                WrappedInvocation.newInvocation(code, ByteBufUtil.retainBuf(data), expires, feedback));
                    }
                } catch (Throwable t) {
                    semaphore.release();
                    throw t;
                }
            } else {
                throw new TimeoutException(STR."Client invoke semaphore acquire timeout[\{timeoutMs}ms]");
            }
        } catch (Throwable t) {
            RuntimeException cause = new RuntimeException(
                    STR."Channel invoke failed, address[\{address}], code[\{code}] and length[\{length}]", t);
            if (callback != null) {
                callback.onCompleted(null, cause);
            } else {
                throw cause;
            }
        } finally {
            ByteBufUtil.release(data);
        }
    }

    @Override
    public void bindTo(@Nonnull MeterRegistry meterRegistry) {

    }

    public String id() {
        return id;
    }

    public Channel channel() {
        return channel;
    }

    public SocketAddress address() {
        return address;
    }

    public EventExecutor executor() {
        return channel.eventLoop();
    }

    public ByteBufAllocator allocator() {
        return channel.alloc();
    }

    public boolean isActive() {
        return channel.isActive();
    }

    public CommandInvoker invoker() {
        return invoker;
    }

    public void close() {
        channel.close();
    }

    public void onClosed(Runnable runnable) {
        channel.closeFuture().addListener(future -> runnable.run());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClientChannel that = (ClientChannel) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public String toString() {
        return STR."(address=\{address}, id='\{id}', channel=\{channel})";
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

}
