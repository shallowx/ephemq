package org.shallow.internal;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;
import org.shallow.ClientConfig;
import org.shallow.util.ObjectUtil;

import java.net.SocketAddress;
import java.util.Objects;

public class ClientChannel{

    private final String name;
    private final Channel channel;
    private final SocketAddress address;
    private final OperationInvoker invoker;

    public ClientChannel(Channel channel, ClientConfig config, SocketAddress address) {
        this.channel = ObjectUtil.checkNotNull(channel, "Channel cannot be null");
        this.name = ObjectUtil.checkNotNull(channel.id().asLongText(), "Name cannot be null");
        this.address = ObjectUtil.checkNotNull(address, "Socket address cannot be null");
        this.invoker = new OperationInvoker(this, config);
    }

    public OperationInvoker invoker() {
        return invoker;
    }

    public Channel channel() {
        return channel;
    }

    public ByteBufAllocator allocator() {
        return channel.alloc();
    }

    public SocketAddress address() {
        return address;
    }

    @SuppressWarnings("unused")
    public EventExecutor eventLoop() {
        return channel.eventLoop();
    }

    public boolean isActive() {
        return channel.isActive();
    }

    @SuppressWarnings("unused")
    public void close() {
        channel.close();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClientChannel that = (ClientChannel) o;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
