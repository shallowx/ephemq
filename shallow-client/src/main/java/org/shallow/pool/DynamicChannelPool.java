package org.shallow.pool;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import org.shallow.internal.ClientChannelInitializer;
import org.shallow.ClientConfig;
import org.shallow.ObjectUtil;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.util.NetworkUtil;
import org.shallow.invoke.ClientChannel;

import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.shallow.util.NetworkUtil.switchSocketAddress;

public class DynamicChannelPool implements ShallowChannelPool {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(DynamicChannelPool.class);

    private final ClientConfig config;
    private final Map<SocketAddress, List<Future<ClientChannel>>> channelPools;
    private final Map<String, Promise<ClientChannel>> assembleChannels;
    private final Bootstrap bootstrap;
    private final ShallowChannelHealthChecker healthChecker;
    private final List<SocketAddress> bootstrapAddress;

    public DynamicChannelPool(Bootstrap bootstrap, ClientConfig config, ShallowChannelHealthChecker healthChecker) {
        this.bootstrap = bootstrap;
        this.config = config;
        this.channelPools = new ConcurrentHashMap<>(config.getBootstrapSocketAddress().size());
        this.assembleChannels = new ConcurrentHashMap<>(config.getBootstrapSocketAddress().size() * config.getChannelPoolCapacity());
        this.healthChecker = healthChecker;
        this.bootstrapAddress = constructBootstrap();
    }

    private List<SocketAddress> constructBootstrap() {
        return ObjectUtil.checkNotNull(NetworkUtil.switchSocketAddress(config.getBootstrapSocketAddress()), "Bootstrap address cannot be null");
    }

    private void constructChannel(SocketAddress address) {
        final List<Future<ClientChannel>> futures = channelPools.computeIfAbsent(address, v -> new CopyOnWriteArrayList<>());
        final int stuff = futures.isEmpty() ? config.getChannelPoolCapacity() : (config.getChannelPoolCapacity() - futures.size());
        for (int i = 0; i < stuff; i++) {
            final Future<ClientChannel> future = newChannel(address);
            futures.add(future);
            future.addListener(f -> {
                if (!f.isSuccess()) {
                    futures.remove(f);
                }
            });
        }
    }

    @Override
    public Future<ClientChannel> acquire(SocketAddress address) {
        Future<ClientChannel> future;
        if (ObjectUtil.isNull(address)) {
            future = randomAcquire();
            if (ObjectUtil.isNotNull(future)) {
                return future;
            }

            address = obtainSocketAddress();
            if (address == null) {
                throw new IllegalArgumentException("Any bootstrap address not found");
            }
        }
        return acquireHealthy(address);
    }

    private Future<ClientChannel> acquireHealthy(final SocketAddress address) {
        return newChannel(address);
    }

    private SocketAddress obtainSocketAddress() {
        final int size = bootstrapAddress.size();
        if (size == 0) {
            return null;
        }

        Optional<SocketAddress> any = bootstrapAddress.parallelStream().findAny();
        return any.orElse(null);
    }

    private Future<ClientChannel> randomAcquire() {
        final int size = channelPools.size();
        if (size == 0) {
            return null;
        }
        Optional<Future<ClientChannel>> any = channelPools.values().parallelStream()
                .flatMap(Collection::stream)
                .filter(healthChecker::isHealthy)
                .findAny();
        return any.orElse(null);
    }

    private Future<ClientChannel> newChannel(SocketAddress address) {
        Bootstrap bs = bootstrap.clone().handler(new ClientChannelInitializer(address, config));
        final ChannelFuture connectFuture = bs.connect(address);
        final Channel channel = connectFuture.channel();
        Promise<ClientChannel> promise = assemblePromise(channel);
        connectFuture.addListener(f -> {
            if (!f.isSuccess()) {
                if (logger.isInfoEnabled()) {
                    logger.info("Failed to new channel", f.cause());
                }
                promise.tryFailure(f.cause());
                assembleChannels.remove(channel.id().asLongText());
            }
        });

        channel.closeFuture().addListener((ChannelFutureListener) f -> {
            promise.tryFailure(new IllegalStateException("Channel is closed"));
            assembleChannels.remove(f.channel().id().asLongText());
        }) ;
        return promise;
    }

    @Override
    public Promise<ClientChannel> assemblePromise(Channel channel) {
        return assembleChannels.computeIfAbsent(channel.id().asLongText(), v -> newImmediatePromise());
    }

    private <T> Promise<T> newImmediatePromise(){
        return ImmediateEventExecutor.INSTANCE.newPromise();
    }
}
