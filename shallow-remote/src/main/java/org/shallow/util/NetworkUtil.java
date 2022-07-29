package org.shallow.util;

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
import io.netty.util.concurrent.*;
import org.shallow.RemoteException;
import org.shallow.codec.MessagePacket;

import javax.naming.OperationNotSupportedException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadFactory;

import static org.shallow.util.ObjectUtil.checkPositive;

public final class NetworkUtil {

    private static final int INT_ZERO = 0;

    private NetworkUtil() throws OperationNotSupportedException {
        // Unused
        throw new OperationNotSupportedException();
    }

    public static MessagePacket newSuccessPacket(int answer, ByteBuf body) {
        return MessagePacket.newPacket(answer, (byte) INT_ZERO, body);
    }

    public static MessagePacket newFailurePacket(int answer, Throwable cause) {
        if (cause instanceof RemoteException e) {
            return MessagePacket.newPacket(answer, e.getCommand(), ByteUtil.string2Buf(e.getMessage()));
        }
        return MessagePacket.newPacket(answer,RemoteException.Failure.UNKNOWN_EXCEPTION, ByteUtil.string2Buf(cause == null ? null : cause.getClass().getSimpleName()));
    }

    public static List<SocketAddress> switchSocketAddress(Collection<? extends String> addresses) {
        final int size = addresses == null ? INT_ZERO : addresses.size();
        if (ObjectUtil.checkPositive(size, "bootstrap address") > INT_ZERO) {
            List<SocketAddress> answers = new LinkedList<>();
            for (String address : addresses) {
                SocketAddress socketAddress = switchSocketAddress(address);
                if (ObjectUtil.isNotNull(socketAddress)) {
                    answers.add(socketAddress);
                }
            }
            return answers;
        }
       return null;
    }

    public static SocketAddress switchSocketAddress(String address) {
        int index = address.lastIndexOf(":");
        if (index < INT_ZERO) {
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
        if (host == null || host.isEmpty() || port < INT_ZERO || port > 0xFFFF) {
            return null;
        }
        return InetSocketAddress.createUnresolved(host, port);
    }

    public static EventLoopGroup newEventLoopGroup(boolean epoll, int threads, String name) {
        ThreadFactory f = new DefaultThreadFactory(name);
        return epoll && Epoll.isAvailable() ? new EpollEventLoopGroup(threads, f) : new NioEventLoopGroup(threads, f);
    }

    public static EventExecutorGroup newEventExecutorGroup(int threads, String groupName) {
        if (threads == 0) {
            threads = Runtime.getRuntime().availableProcessors();
        }

        ThreadFactory f = new DefaultThreadFactory(groupName);
        return new DefaultEventExecutorGroup(threads, f);
    }

    public static Class<? extends Channel> preferChannelClass(boolean epoll) {
        return epoll && Epoll.isAvailable() ? EpollSocketChannel.class : NioSocketChannel.class;
    }

    public static Class<? extends ServerChannel> preferServerChannelClass(boolean epoll) {
        return epoll && Epoll.isAvailable() ? EpollServerSocketChannel.class : NioServerSocketChannel.class;
    }

    public static String switchAddress(Channel channel) {
        return ((channel == null ? null : channel.remoteAddress()) == null ? null : channel.remoteAddress().toString());
    }

    public static  <T> Promise<T> newImmediatePromise() {
        return ImmediateEventExecutor.INSTANCE.newPromise();
    }
}
