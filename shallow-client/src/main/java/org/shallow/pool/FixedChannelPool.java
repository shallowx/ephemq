package org.shallow.pool;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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

    @Override
    public void initChannelPool() {
        if (bootstrapAddress == null) {
            throw new IllegalArgumentException("Initialize client bootstrap address cannot be null");
        }

        Iterator<SocketAddress> iterator = bootstrapAddress.iterator();
        int capacity = client.getClientConfig().getChannelFixedPoolCapacity();
        while (iterator.hasNext()) {
            SocketAddress address = iterator.next();
            for (int i = 0; i < capacity; i++) {
                try {
                    Future<ClientChannel> future = newChannel(address);
                    channelPools.computeIfAbsent(address, v -> new CopyOnWriteArrayList<>()).add(future);

                    future.addListener((GenericFutureListener<Future<ClientChannel>>) f -> {
                        if (!f.isSuccess()) {
                            remove(address, f);
                        } else {
                            ClientChannel clientChannel = f.getNow();
                            clientChannel.channel().closeFuture().addListener(cf -> remove(address, f));
                        }
                    });
                } catch (Throwable t) {
                    if (logger.isErrorEnabled()) {
                        logger.error("Failed to init chanel pool<{}>, address={}", client.getName(), address.toString());
                    }
                }
            }
        }
    }

    private void remove(SocketAddress address, Future<ClientChannel> future) {
        synchronized (channelPools) {
            List<Future<ClientChannel>> futures = channelPools.get(address);
            if (futures == null) {
                return;
            }

            if (logger.isDebugEnabled()) {
                logger.debug("Remove {} from chanel pool<{}>", address, client.getName());
            }

            futures.remove(future);
            if (futures.isEmpty()) {
                channelPools.remove(address);
            }
        }
    }

    private List<SocketAddress> constructBootstrap() {
        return ObjectUtil.checkNotNull(switchSocketAddress(clientConfig.getBootstrapSocketAddress()), "Client bootstrap address cannot be null");
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

        List<Future<ClientChannel>> futures = acquireHealthy(address);
        if (futures != null && !futures.isEmpty()) {
            if (futures.size() == 1) {
                return futures.get(0);
            }
            return futures.get(ThreadLocalRandom.current().nextInt(futures.size()));
        }

        synchronized (channelPools) {
            List<Future<ClientChannel>> actives = acquireHealthy(address);
            if (actives != null && !actives.isEmpty()) {
                if (actives.size() == 1) {
                    return actives.get(0);
                }
                return actives.get(ThreadLocalRandom.current().nextInt(actives.size()));
            }

            channelPools.remove(address);
            for (int i = 0; i < client.getClientConfig().getChannelFixedPoolCapacity(); i++) {
                try {
                    Future<ClientChannel> future = newChannel(address);
                    channelPools.computeIfAbsent(address, v -> new CopyOnWriteArrayList<>()).add(future);

                    future.addListener((GenericFutureListener<Future<ClientChannel>>) f -> {
                        if (!f.isSuccess()) {
                            remove(address, f);
                        } else {
                            ClientChannel clientChannel = f.getNow();
                            clientChannel.channel().closeFuture().addListener(cf -> remove(address, f));
                        }
                    });
                } catch (Throwable t) {
                   if (logger.isErrorEnabled()) {
                       logger.error("Failed to acquire healthy chanel from pool<{}>, address={}", client.getName(), address.toString());
                   }
                 }
            }
        }
        List<Future<ClientChannel>> activeFutures = channelPools.get(address);
        return activeFutures.get(ThreadLocalRandom.current().nextInt(activeFutures.size()));
    }

    private List<Future<ClientChannel>> acquireHealthy(final SocketAddress address) {
        List<Future<ClientChannel>> futures = channelPools.get(address);
        if (futures == null || futures.isEmpty()) {
            return null;
        }
        return futures.stream().filter(healthChecker::isHealthy).collect(Collectors.toCollection(CopyOnWriteArrayList::new));
    }

    private Future<ClientChannel> randomAcquire() {
        Future<ClientChannel> future = null;
        List<SocketAddress> addresses = constructBootstrap();
        SocketAddress socketAddress = addresses.get(ThreadLocalRandom.current().nextInt(addresses.size()));
        List<Future<ClientChannel>> futures = channelPools.get(socketAddress);

        if (futures != null) {
            List<Future<ClientChannel>> actives = futures.stream()
                    .filter(healthChecker::isHealthy).collect(Collectors.toCollection(CopyOnWriteArrayList::new));

            if (!actives.isEmpty()) {
                if (actives.size() == 1) {
                    return actives.get(0);
                }
                return actives.get(ThreadLocalRandom.current().nextInt(actives.size()));
            }
        }

        synchronized (channelPools) {
            List<Future<ClientChannel>> listFutures = channelPools.get(socketAddress);
            if (listFutures != null && !listFutures.isEmpty()) {
                if (listFutures.size() == 1) {
                    return listFutures.get(0);
                }
                return listFutures.get(ThreadLocalRandom.current().nextInt(listFutures.size()));
            }

            for (int i = 0; i < client.getClientConfig().getChannelFixedPoolCapacity(); i++) {
                future = newChannel(socketAddress);
                channelPools.computeIfAbsent(socketAddress, v -> new CopyOnWriteArrayList<>()).add(future);

                future.addListener((GenericFutureListener<Future<ClientChannel>>) f -> {
                    if (!f.isSuccess()) {
                        remove(socketAddress, f);
                    } else {
                        ClientChannel clientChannel = f.getNow();
                        clientChannel.channel().closeFuture().addListener(cf -> remove(socketAddress, f));
                    }
                });
            }
        }

        return future;
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
        client.shutdownGracefully();
        channelPools.clear();
        assembleChannels.clear();
    }
}
