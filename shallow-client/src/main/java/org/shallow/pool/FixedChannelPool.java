package org.shallow.pool;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.util.concurrent.DefaultThreadFactory;
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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
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
    private final ScheduledExecutorService scheduledExecutor;
    private final AtomicBoolean flipState = new AtomicBoolean(false);

    public FixedChannelPool(Client client, ShallowChannelHealthChecker healthChecker) {
        this.client = client;
        this.bootstrap = client.getBootstrap();
        this.clientConfig = client.getClientConfig();
        this.channelPools = new ConcurrentHashMap<>(clientConfig.getBootstrapSocketAddress().size());
        this.assembleChannels = new ConcurrentHashMap<>(clientConfig.getBootstrapSocketAddress().size() * clientConfig.getChannelFixedPoolCapacity());
        this.healthChecker = healthChecker;
        this.bootstrapAddress = constructBootstrap();
        this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory(String.format("%s-channel-poll", client.getName())));

        scheduledExecutor.schedule(this::checkHealthyChannel, 5000, TimeUnit.MILLISECONDS);
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
            flip(address, capacity);
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
            flip(address, client.getClientConfig().getChannelFixedPoolCapacity());
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
        Future<ClientChannel> future;
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

            future = flip(socketAddress, client.getClientConfig().getChannelFixedPoolCapacity());
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

    private void checkHealthyChannel() {
        if (channelPools.isEmpty()) {
            return;
        }

        Set<Map.Entry<SocketAddress, List<Future<ClientChannel>>>> entries = channelPools.entrySet();
        for (Map.Entry<SocketAddress, List<Future<ClientChannel>>> entry : entries) {
            List<Future<ClientChannel>> listFuture = entry.getValue();
            List<Future<ClientChannel>> activeFutures = listFuture.stream()
                    .filter(healthChecker::isHealthy)
                    .collect(Collectors.toCollection(CopyOnWriteArrayList::new));

            int diff = client.getClientConfig().getChannelFixedPoolCapacity() - activeFutures.size();
            if (diff == 0) {
                continue;
            }

            SocketAddress address = entry.getKey();
            flip(address, diff);
        }
    }

    private Future<ClientChannel> flip(SocketAddress address, int limit) {
        if (address == null || limit <= 0) {
            throw new IllegalArgumentException("Address cannot be null, or limit expect > 0");
        }

        // invoker confirms safety synchronized, @see org.shallow.pool.FixedChannelPool#acquireHealthyOrNew0
        if (flipState.compareAndSet(false, true)) {
            throw new UnsupportedOperationException();
        }

        Future<ClientChannel> future = null;
        for (int i = 0; i < limit; i++) {
            try {
                future = newChannel(address);
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
                    logger.error("Failed to new healthy chanel<{}>, address={}", client.getName(), address.toString());
                }
            }
        }
        flipState.compareAndSet(true, false);
        return future;
    }

    @Override
    public Promise<ClientChannel> assemblePromise(Channel channel) {
        return assembleChannels.computeIfAbsent(channel.id().asLongText(), v -> newImmediatePromise());
    }

    @Override
    public void shutdownGracefully() throws Exception {
        if (!scheduledExecutor.isShutdown() || !scheduledExecutor.isTerminated()) {
            scheduledExecutor.shutdown();
        }
        client.shutdownGracefully();
        channelPools.clear();
        assembleChannels.clear();
    }
}
