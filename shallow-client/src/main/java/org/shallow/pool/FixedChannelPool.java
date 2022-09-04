package org.shallow.pool;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import org.shallow.Client;
import org.shallow.internal.ClientChannelInitializer;
import org.shallow.ClientConfig;
import org.shallow.util.ObjectUtil;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.internal.ClientChannel;

import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.shallow.util.NetworkUtil.newImmediatePromise;
import static org.shallow.util.NetworkUtil.switchSocketAddress;

public class FixedChannelPool implements ShallowChannelPool {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(FixedChannelPool.class);

    private final Client client;
    private final Bootstrap bootstrap;
    private final ClientConfig clientConfig;
    private final Map<SocketAddress, List<Future<ClientChannel>>> channelPools;
    private final Map<String, Promise<ClientChannel>> assembleChannels;
    private final ShallowChannelHealthChecker healthChecker;
    private final List<SocketAddress> bootstrapAddress;

    public FixedChannelPool(Client client, ShallowChannelHealthChecker healthChecker) {
        this.client = client;
        this.bootstrap = client.getBootstrap();
        this.clientConfig = client.getClientConfig();
        this.channelPools = new ConcurrentHashMap<>(clientConfig.getBootstrapSocketAddress().size());
        this.assembleChannels = new ConcurrentHashMap<>(clientConfig.getBootstrapSocketAddress().size() * clientConfig.getChannelFixedPoolCapacity());
        this.healthChecker = healthChecker;
        this.bootstrapAddress = constructBootstrap();
    }

    private List<SocketAddress> constructBootstrap() {
        return ObjectUtil.checkNotNull(switchSocketAddress(clientConfig.getBootstrapSocketAddress()), "Client bootstrap address cannot be empty");
    }

    @Override
    public ClientChannel acquireHealthyOrNew(SocketAddress address) {
        try {
            return acquireHealthyOrNew0(address).get(clientConfig.getConnectTimeOutMs(), TimeUnit.MILLISECONDS);
        } catch (Throwable t) {
            throw new RuntimeException("Failed to get healthy channel from pool", t);
        }
    }

    @Override
    public ClientChannel acquireWithRandomly() {
        try {
            return randomAcquire().get(clientConfig.getConnectTimeOutMs(), TimeUnit.MILLISECONDS);
        } catch (Throwable t) {
            throw new RuntimeException("Failed to get healthy channel randomly from pool", t);
        }
    }

    private Future<ClientChannel> acquireHealthyOrNew0(SocketAddress address) {
        if (null == address) {
            throw new IllegalArgumentException("Any bootstrap address not found");
        }

        return acquireHealthy(address);
    }

    private Future<ClientChannel> acquireHealthy(final SocketAddress address) {
        List<Future<ClientChannel>> futures = channelPools.get(address);
        if (futures == null || futures.isEmpty()) {
            synchronized (channelPools) {
                List<Future<ClientChannel>> activeFutures = channelPools.get(address);
                if (activeFutures == null || activeFutures.isEmpty()) {
                    Future<ClientChannel> future = newChannel(address);
                    channelPools.put(address, List.of(future));
                }
            }
        }
        return channelPools.get(address).get(0);
    }

    private SocketAddress toSocketAddress() {
        final int size = bootstrapAddress.size();
        if (size == 0) {
            return null;
        }

        Optional<SocketAddress> any = bootstrapAddress.parallelStream().findAny();
        return any.orElse(null);
    }

    private Future<ClientChannel> randomAcquire() {
        return newChannel(toSocketAddress());
    }

    private Future<ClientChannel> newChannel(SocketAddress address) {
        Bootstrap bs = bootstrap.clone().handler(new ClientChannelInitializer(address, client));
        final ChannelFuture connectFuture = bs.connect(address);
        final Channel channel = connectFuture.channel();
        Promise<ClientChannel> promise = assemblePromise(channel);
        connectFuture.addListener(f -> {
            if (!f.isSuccess()) {
                if (logger.isErrorEnabled()) {
                    logger.error("Failed to new channel with cause:{}", f.cause());
                }
                promise.tryFailure(f.cause());
                assembleChannels.remove(channel.id().asLongText());
            }
        });

        channel.closeFuture().addListener((ChannelFutureListener) f -> {
            promise.tryFailure(new IllegalStateException(String.format("Channel<%s> was closed", f.channel().toString())));
            assembleChannels.remove(f.channel().id().asLongText());
        }) ;
        return promise;
    }

    @Override
    public Promise<ClientChannel> assemblePromise(Channel channel) {
        return assembleChannels.computeIfAbsent(channel.id().asLongText(), v -> newImmediatePromise());
    }

    @Override
    public void shutdownGracefully() throws Exception {

    }
}
