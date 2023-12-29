package org.meteor.client.internal;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.EventExecutor;
import org.meteor.remote.invoke.Callback;
import org.meteor.remote.invoke.GenericInvokeAnswer;
import org.meteor.remote.invoke.InvokeAnswer;
import org.meteor.remote.processor.AwareInvocation;
import org.meteor.remote.util.ByteBufUtil;

import javax.annotation.Nonnull;
import java.net.SocketAddress;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ClientChannel implements MeterBinder {
    protected static final String CHANNEL_SEMAPHORE = "channel_semaphore";
    private final SocketAddress address;
    private final CommandInvoker invoker;
    protected String id;
    protected Channel channel;
    protected Semaphore semaphore;

    public ClientChannel(ClientConfig config, Channel channel, SocketAddress address) {
        this.id = channel.id().asLongText();
        this.channel = channel;
        this.address = address;
        this.semaphore = new Semaphore(config.getChannelInvokePermits());
        this.invoker = new CommandInvoker(this);
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
        return "ClientChannel{" +
                "address=" + address+
                ", id='" + id + '\'' +
                ", channel=" + channel +
                '}';
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    public void invoke(int code, ByteBuf data, int timeoutMs, Callback<ByteBuf> callback) {
        int length = ByteBufUtil.bufLength(data);
        try {
            long time = System.currentTimeMillis();
            if (semaphore.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS)) {
                try {
                    if (callback == null) {
                        ChannelPromise promise = channel.newPromise().addListener(f -> semaphore.release());
                        channel.writeAndFlush(AwareInvocation.newInvocation(code, ByteBufUtil.retainBuf(data)), promise);
                    } else {
                        long expires = timeoutMs + time;
                        InvokeAnswer<ByteBuf> answer = new GenericInvokeAnswer<>((v, c) -> {
                            semaphore.release();
                            callback.operationCompleted(v, c);
                        });
                        channel.writeAndFlush(AwareInvocation.newInvocation(code, ByteBufUtil.retainBuf(data), expires, answer));
                    }
                } catch (Throwable t) {
                    semaphore.release();
                    throw t;
                }
            } else {
                throw new TimeoutException("Client invoke semaphore acquire timeout" + timeoutMs + "ms");
            }
        } catch (Throwable t) {
            RuntimeException cause = new RuntimeException(
                    String.format("Channel invoke failed, address=%s code=%d length=%d", address(), code, length), t
            );
            if (callback != null) {
                callback.operationCompleted(null, cause);
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
}
