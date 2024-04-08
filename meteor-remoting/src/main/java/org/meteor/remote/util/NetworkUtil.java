package org.meteor.remote.util;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.RejectedExecutionHandlers;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import org.meteor.common.thread.FastEventExecutorGroup;
import org.meteor.common.util.ObjectUtil;
import org.meteor.remote.codec.MessagePacket;
import org.meteor.remote.invoke.RemoteException;

public final class NetworkUtil {
    private NetworkUtil() {
        throw new AssertionError("No org.meteor.remote.util.NetworkUtil instance for you");
    }

    public static MessagePacket newSuccessPacket(long feedback, ByteBuf body) {
        return MessagePacket.newPacket(feedback, 0, body);
    }

    public static MessagePacket newFailurePacket(long feedback, Throwable cause) {
        if (cause instanceof RemoteException e) {
            return MessagePacket.newPacket(feedback, e.getCommand(), ByteBufUtil.string2Buf(e.getMessage()));
        }

        return MessagePacket.newPacket(feedback, RemoteException.Failure.UNKNOWN_EXCEPTION,
                ByteBufUtil.string2Buf(cause == null ? null : cause.getMessage()));
    }

    public static List<SocketAddress> switchSocketAddress(Collection<? extends String> addresses) {
        final int size = addresses == null ? 0 : addresses.size();
        if (ObjectUtil.checkPositive(size, "bootstrap address") > 0) {
            List<SocketAddress> socketAddresses = new LinkedList<>();
            for (String address : addresses) {
                SocketAddress socketAddress = switchSocketAddress(address);
                if (null != socketAddress) {
                    socketAddresses.add(socketAddress);
                }
            }
            return socketAddresses;
        }
        return null;
    }

    public static SocketAddress switchSocketAddress(String address) {
        int index = address.lastIndexOf(":");
        if (index < 0) {
            return null;
        }

        String host = address.substring(0, index);
        int port;
        try {
            port = Integer.parseInt(address.substring(index + 1));
        } catch (Exception e) {
            return null;
        }

        return switchSocketAddress(host, port);
    }

    public static SocketAddress switchSocketAddress(String host, int port) {
        host = host == null ? null : host.trim();
        if (host == null || host.isEmpty()
                || port < 0 || port > 0xFFFF) {
            return null;
        }

        return InetSocketAddress.createUnresolved(host, port);
    }

    public static EventLoopGroup newEventLoopGroup(boolean epoll, int threads, String name) {
        ThreadFactory f = new DefaultThreadFactory(name);
        return epoll && Epoll.isAvailable()
                ? new EpollEventLoopGroup(threads, f)
                : new NioEventLoopGroup(threads, f);
    }

    public static EventExecutorGroup newEventExecutorGroup(int threads, String groupName) {
        if (threads == 0) {
            threads = Runtime.getRuntime().availableProcessors();
        }

        ThreadFactory f = new DefaultThreadFactory(groupName);
        return new FastEventExecutorGroup(threads, f);
    }

    @SuppressWarnings("unused")
    public static EventExecutorGroup newEventExecutorGroup(int threads, int maxPendingTasks, String poolName) {
        if (threads == 0) {
            threads = Runtime.getRuntime().availableProcessors();
        }

        ThreadFactory f = new DefaultThreadFactory(poolName);
        return new FastEventExecutorGroup(threads, f, true, maxPendingTasks, RejectedExecutionHandlers.reject());
    }

    public static Class<? extends Channel> preferChannelClass(boolean epoll) {
        return epoll && Epoll.isAvailable()
                ? EpollSocketChannel.class
                : NioSocketChannel.class;
    }

    public static Class<? extends ServerChannel> preferServerChannelClass(boolean epoll) {
        return epoll && Epoll.isAvailable()
                ? EpollServerSocketChannel.class
                : NioServerSocketChannel.class;
    }

    public static String switchAddress(Channel channel) {
        return ((channel == null ? null : channel.remoteAddress()) == null
                ? null :
                channel.remoteAddress().toString());
    }

    @SuppressWarnings("unused")
    public static <T> Promise<T> newImmediatePromise() {
        return ImmediateEventExecutor.INSTANCE.newPromise();
    }
}
