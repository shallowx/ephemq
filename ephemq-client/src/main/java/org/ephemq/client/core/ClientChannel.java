package org.ephemq.client.core;

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
import org.ephemq.remote.invoke.Callable;
import org.ephemq.remote.invoke.GenericInvokedFeedback;
import org.ephemq.remote.invoke.InvokedFeedback;
import org.ephemq.remote.invoke.WrappedInvocation;
import org.ephemq.remote.util.ByteBufUtil;

/**
 * The ClientChannel class manages a client connection using a Netty Channel.
 * It is responsible for invoking commands, handling semaphore for controlling
 * concurrent invocations, and binding metrics to a MeterRegistry.
 */
public class ClientChannel implements MeterBinder {
    /**
     * The network address associated with the client channel.
     */
    private final SocketAddress address;
    /**
     * An instance of the CommandInvoker used to send and query commands over the client's channel.
     * This handles the sending of various requests and managing their responses.
     */
    private final CommandInvoker invoker;
    /**
     * A unique identifier for the ClientChannel instance.
     */
    protected String id;
    /**
     * The Netty {@link Channel} associated with the client connection.
     * This channel facilitates communication between the client and server.
     * It is used by the command invoker to send and receive messages,
     * and manage protocol-specific communication tasks.
     */
    protected Channel channel;
    /**
     * A semaphore to manage the concurrency of operations in the ClientChannel.
     * It is used to limit the number of concurrent invocations to the remote server,
     * ensuring that the ClientChannel does not overwhelm the server with too many
     * requests at once.
     */
    protected Semaphore semaphore;
    /**
     * Hash code computed for this ClientChannel instance. It is a final integer field
     * used to store the hash value, enhancing the performance of hash-based collections
     * that this object may be part of.
     */
    private final int hashCode;

    /**
     * Constructs a new ClientChannel with the specified configuration, channel, and address.
     *
     * @param config  the configuration for the client channel
     * @param channel the channel associated with the client
     * @param address the socket address for the channel
     */
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
                throw new TimeoutException(String.format("Client invoke semaphore acquire timeout[%d ms]", timeoutMs));
            }
        } catch (Throwable t) {
            RuntimeException cause = new RuntimeException(String.format("Channel invoke failed, address[%s], code[%s] and length[%d]", address, code, length), t);
            if (callback != null) {
                callback.onCompleted(null, cause);
            } else {
                throw cause;
            }
        } finally {
            ByteBufUtil.release(data);
        }
    }

    /**
     * Binds the given MeterRegistry to this ClientChannel for the purpose of
     * recording metrics.
     *
     * @param meterRegistry the MeterRegistry to bind to this ClientChannel,
     *                      which must not be null.
     */
    @Override
    public void bindTo(@Nonnull MeterRegistry meterRegistry) {

    }

    /**
     * Gets the unique identifier for this client channel.
     *
     * @return the unique identifier of this client channel as a String.
     */
    public String id() {
        return id;
    }

    /**
     * Returns the current Channel associated with this ClientChannel.
     *
     * @return the current Channel
     */
    public Channel channel() {
        return channel;
    }

    /**
     * Retrieves the socket address associated with this client channel.
     *
     * @return the socket address connected to this client channel.
     */
    public SocketAddress address() {
        return address;
    }

    /**
     * Returns the EventExecutor associated with the channel.
     *
     * @return the EventExecutor associated with the channel.
     */
    public EventExecutor executor() {
        return channel.eventLoop();
    }

    /**
     * Returns the ByteBufAllocator associated with the current channel.
     *
     * @return the ByteBufAllocator instance used by the channel.
     */
    public ByteBufAllocator allocator() {
        return channel.alloc();
    }

    /**
     * Checks if the underlying channel is currently active.
     *
     * @return true if the channel is active, false otherwise.
     */
    public boolean isActive() {
        return channel.isActive();
    }

    /**
     * Returns the command invoker associated with this client channel.
     *
     * @return the {@link CommandInvoker} instance associated with this client channel.
     */
    public CommandInvoker invoker() {
        return invoker;
    }

    /**
     * Closes the client channel.
     * <p>
     * This method invokes the underlying Netty channel's close method to
     * release resources and terminate the connection.
     */
    public void close() {
        channel.close();
    }

    /**
     * Registers a callback to be invoked when the channel is closed.
     *
     * @param runnable the callback to be executed upon channel closure
     */
    public void onClosed(Runnable runnable) {
        channel.closeFuture().addListener(future -> runnable.run());
    }

    /**
     * Compares this ClientChannel with another object for equality.
     * Two ClientChannel objects are considered equal if they have the same id.
     *
     * @param o the object to compare this ClientChannel against
     * @return true if the given object is equal to this ClientChannel; false otherwise
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClientChannel that = (ClientChannel) o;
        return Objects.equals(id, that.id);
    }

    /**
     * Returns a string representation of the ClientChannel instance.
     *
     * @return a string representation of the ClientChannel instance including address, id, and channel details.
     */
    @Override
    public String toString() {
        return "ClientChannel (address=%s, invoker=%s, id='%s', channel=%s, semaphore=%s, hashCode=%d)".formatted(address, invoker, id, channel, semaphore, hashCode);
    }

    /**
     * Returns the hash code for this ClientChannel instance. The hash code is
     * based on the internal state of the ClientChannel and can be used to
     * uniquely identify this instance.
     *
     * @return the hash code for this ClientChannel instance.
     */
    @Override
    public int hashCode() {
        return hashCode;
    }

}
