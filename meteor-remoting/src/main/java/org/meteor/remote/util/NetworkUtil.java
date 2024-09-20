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
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import net.openhft.affinity.AffinityStrategies;
import net.openhft.affinity.AffinityThreadFactory;
import org.meteor.common.thread.FastEventExecutorGroup;
import org.meteor.common.util.ObjectUtil;
import org.meteor.remote.codec.MessagePacket;
import org.meteor.remote.exception.RemotingException;

/**
 * Utility class providing various network-related operations.
 * <p>
 * This class contains static methods to create various network packets, convert string addresses
 * to socket addresses, instantiate event loop groups, and determine preferred channel classes.
 */
public final class NetworkUtil {
    /**
     * Utility class for network-related operations.
     * This class is not meant to be instantiated.
     * The constructor throws an AssertionError if called to prevent instantiation.
     */
    private NetworkUtil() {
        throw new AssertionError("No org.meteor.remote.util.NetworkUtil instance for you");
    }

    /**
     * Creates a new success MessagePacket with the specified feedback and body content.
     *
     * @param feedback The feedback associated with the message packet.
     * @param body The ByteBuf containing the body content of the message packet.
     * @return A newly created success MessagePacket.
     */
    public static MessagePacket newSuccessPacket(long feedback, ByteBuf body) {
        return MessagePacket.newPacket(feedback, 0, body);
    }

    /**
     * Creates a new failure {@link MessagePacket} based on the given feedback and cause.
     *
     * @param feedback The feedback identifier associated with the failure.
     * @param cause The throwable cause of the failure.
     * @return A new {@link MessagePacket} representing the failure.
     */
    public static MessagePacket newFailurePacket(long feedback, Throwable cause) {
        if (cause instanceof RemotingException e) {
            return MessagePacket.newPacket(feedback, e.getCommand(), ByteBufUtil.string2Buf(e.getMessage()));
        }

        return MessagePacket.newPacket(feedback, RemotingException.Failure.UNKNOWN_EXCEPTION,
                ByteBufUtil.string2Buf(cause == null ? null : cause.getMessage()));
    }

    /**
     * Converts a collection of string representations of addresses to a list of SocketAddress objects.
     *
     * @param addresses the collection of string addresses to be converted
     * @return a list of SocketAddress objects, or null if the input collection is null or empty
     */
    public static List<SocketAddress> switchSocketAddress(Collection<? extends String> addresses) {
        final int size = addresses == null ? 0 : addresses.size();
        if (ObjectUtil.checkPositive(size, "bootstrap address") > 0) {
            List<SocketAddress> socketAddresses = new ObjectArrayList<>();
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

    /**
     * Converts a string representation of an address into a SocketAddress.
     * The address should be in the format "host:port".
     *
     * @param address the string representation of the address.
     * @return the corresponding SocketAddress, or null if the input format is invalid.
     */
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

    /**
     * Converts a host and port into a SocketAddress.
     *
     * @param host The hostname to be used for the SocketAddress. If the host is null or empty after trimming, the method returns null.
     * @param port The port number to be used for the SocketAddress. If the port is out of the valid range (0-65535), the method returns null.
     * @return A SocketAddress object representing the given host and port, or null if the input values are invalid.
     */
    public static SocketAddress switchSocketAddress(String host, int port) {
        host = host == null ? null : host.trim();
        if (host == null || host.isEmpty()
                || port < 0 || port > 0xFFFF) {
            return null;
        }

        return InetSocketAddress.createUnresolved(host, port);
    }

    /**
     * Creates a new {@link EventLoopGroup} based on the specified parameters.
     *
     * @param epoll      Determines whether to use Epoll for the EventLoopGroup. If true, and if Epoll is available,
     *                   an EpollEventLoopGroup will be created; otherwise, a NioEventLoopGroup will be created.
     * @param threads    The number of threads to use for the EventLoopGroup.
     * @param name       The name prefix to use for the threads in the EventLoopGroup.
     * @param isAffinity Indicates whether thread affinity should be applied. If true,
     *                   an AffinityThreadFactory will be used; otherwise, a DefaultThreadFactory will be used.
     *
     * @return A newly created {@link EventLoopGroup} configured based on the specified parameters.
     */
    // https://netty.io/wiki/thread-affinity.html
    public static EventLoopGroup newEventLoopGroup(boolean epoll, int threads, String name, boolean isAffinity) {
        ThreadFactory f = isAffinity
                ? new AffinityThreadFactory(name, AffinityStrategies.DIFFERENT_CORE)
                : new DefaultThreadFactory(name);

        return epoll && Epoll.isAvailable()
                ? new EpollEventLoopGroup(threads, f)
                : new NioEventLoopGroup(threads, f);
    }

    /**
     * Creates a new EventExecutorGroup with a specified number of threads and a group name.
     *
     * @param threads the number of threads to be used by the EventExecutorGroup. If set to 0, the number of
     *                threads will default to the number of available processors.
     * @param groupName the name of the thread group to be created.
     * @return a new instance of FastEventExecutorGroup configured with the specified number of threads
     *         and thread group name.
     */
    public static EventExecutorGroup newEventExecutorGroup(int threads, String groupName) {
        if (threads == 0) {
            threads = Runtime.getRuntime().availableProcessors();
        }

        ThreadFactory f = new DefaultThreadFactory(groupName);
        return new FastEventExecutorGroup(threads, f);
    }

    /**
     * Creates a new EventExecutorGroup with a specified number of threads,
     * maximum pending tasks, and a thread pool name.
     *
     * @param threads the number of threads to be used by the executor group.
     *                If set to 0, the number of available processors will be used.
     * @param maxPendingTasks the maximum number of pending tasks allowed in the queue.
     * @param poolName the name of the thread pool associated with this executor group.
     * @return a new FastEventExecutorGroup instance configured with the specified parameters.
     */
    @SuppressWarnings("unused")
    public static EventExecutorGroup newEventExecutorGroup(int threads, int maxPendingTasks, String poolName) {
        if (threads == 0) {
            threads = Runtime.getRuntime().availableProcessors();
        }

        ThreadFactory f = new DefaultThreadFactory(poolName);
        return new FastEventExecutorGroup(threads, f, true, maxPendingTasks, RejectedExecutionHandlers.reject());
    }

    /**
     * Returns the preferred {@link Channel} class based on the specified epoll availability.
     *
     * @param epoll a boolean indicating whether epoll should be preferred if available
     * @return the class of the preferred {@link Channel}; either {@link EpollSocketChannel} if epoll is available and preferred,
     * or {@link NioSocketChannel} if epoll is not available or not preferred
     */
    public static Class<? extends Channel> preferChannelClass(boolean epoll) {
        return epoll && Epoll.isAvailable()
                ? EpollSocketChannel.class
                : NioSocketChannel.class;
    }

    /**
     * Chooses the preferred {@link ServerChannel} class based on the availability of Epoll.
     *
     * @param epoll A boolean indicating whether Epoll should be preferred.
     * @return The preferred ServerChannel class, either {@link EpollServerSocketChannel} if Epoll is available,
     * or {@link NioServerSocketChannel} otherwise.
     */
    public static Class<? extends ServerChannel> preferServerChannelClass(boolean epoll) {
        return epoll && Epoll.isAvailable()
                ? EpollServerSocketChannel.class
                : NioServerSocketChannel.class;
    }

    /**
     * Retrieves and converts the remote address of the given channel to a string.
     *
     * @param channel the Channel from which to get the remote address; may be null
     * @return the string representation of the remote address, or null if the channel or its remote address is null
     */
    public static String switchAddress(Channel channel) {
        return ((channel == null ? null : channel.remoteAddress()) == null
                ? null :
                channel.remoteAddress().toString());
    }

    /**
     * Creates a new immediate promise using the ImmediateEventExecutor.
     *
     * @param <T> the type parameter for the promise.
     * @return a new Promise instance.
     */
    @SuppressWarnings("unused")
    public static <T> Promise<T> newImmediatePromise() {
        return ImmediateEventExecutor.INSTANCE.newPromise();
    }
}
