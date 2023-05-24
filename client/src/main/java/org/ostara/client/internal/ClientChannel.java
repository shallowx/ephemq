package org.ostara.client.internal;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.EventExecutor;
import org.ostara.client.ClientConfig;
import org.ostara.remote.invoke.Callback;
import org.ostara.remote.invoke.GenericInvokeAnswer;
import org.ostara.remote.invoke.InvokeAnswer;
import org.ostara.remote.processor.AwareInvocation;
import org.ostara.remote.util.ByteBufUtils;

import java.net.SocketAddress;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ClientChannel implements MeterBinder {
    protected String id;
    protected Channel channel;
    private SocketAddress address;
    protected Semaphore semaphore;
    private CommandInvoker invoker;

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
        channel.closeFuture().addListener(future ->  runnable.run());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClientChannel that = (ClientChannel) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    public void invoke(int code, ByteBuf data, int timeoutMs, Callback<ByteBuf> callback) {
        int length = ByteBufUtils.bufLength(data);
        try{
           long time = System.currentTimeMillis();
           if (semaphore.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS)) {
               try {
                   if (callback == null) {
                       ChannelPromise promise = channel.newPromise().addListener(f -> semaphore.release());
                       channel.writeAndFlush(AwareInvocation.newInvocation(code, ByteBufUtils.retainBuf(data)), promise);
                   } else {
                       long expires = timeoutMs + time;
                       InvokeAnswer<ByteBuf> answer = new GenericInvokeAnswer<>((v, c) -> {
                           semaphore.release();
                           callback.operationCompleted(v, c);
                       });
                       channel.writeAndFlush(AwareInvocation.newInvocation(code, ByteBufUtils.retainBuf(data), expires, answer));
                   }
               } catch (Throwable t){
                   semaphore.release();
                   throw t;
               }
           } else {
               throw new TimeoutException("Client invoke semaphore acquire timeout" + timeoutMs + "ms");
           }
        }catch (Throwable t){
            RuntimeException cause = new RuntimeException(
                    String.format("Channel invoke failed, address=%s code=%d length=%d", address(), code, length), t
            );
            if (callback != null) {
                callback.operationCompleted(null, cause);
            } else {
                throw cause;
            }
        } finally {
            ByteBufUtils.release(data);
        }
    }

    protected static final String CHANNEL_SEMAPHORE = "channel_semaphore";
    @Override
    public void bindTo(MeterRegistry meterRegistry) {

    }
}
