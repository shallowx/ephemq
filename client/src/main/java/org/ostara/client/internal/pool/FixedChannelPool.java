package org.ostara.client.internal.pool;

import static org.ostara.remote.util.NetworkUtils.newImmediatePromise;
import static org.ostara.remote.util.NetworkUtils.switchSocketAddress;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import java.net.SocketAddress;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.ostara.client.internal.Client;
import org.ostara.client.internal.ClientChannel;
import org.ostara.client.internal.ClientChannelInitializer;
import org.ostara.client.internal.ClientConfig;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.common.util.ObjectUtils;

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
        this.assembleChannels = new ConcurrentHashMap<>(
                clientConfig.getBootstrapSocketAddress().size() * clientConfig.getChannelFixedPoolCapacity());
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
        return ObjectUtils.checkNotNull(switchSocketAddress(clientConfig.getBootstrapSocketAddress()),
                "Client bootstrap address cannot be null");
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
        } else {
            synchronized (channelPools) {
                List<Future<ClientChannel>> actives = acquireHealthy(address);
                if (actives != null && !actives.isEmpty()) {
                    if (actives.size() == 1) {
                        return actives.get(0);
                    }
                    return actives.get(ThreadLocalRandom.current().nextInt(actives.size()));
                }
                flip(address, client.getClientConfig().getChannelFixedPoolCapacity());
            }
            List<Future<ClientChannel>> activeFutures = channelPools.get(address);
            if (activeFutures.size() == 1) {
                return activeFutures.get(ThreadLocalRandom.current().nextInt(0));
            }

            return activeFutures.get(ThreadLocalRandom.current().nextInt(activeFutures.size()));
        }
    }

    private List<Future<ClientChannel>> acquireHealthy(final SocketAddress address) {
        List<Future<ClientChannel>> futures = channelPools.get(address);
        if (futures == null || futures.isEmpty()) {
            return null;
        }
        return futures.stream().filter(healthChecker::isHealthy)
                .collect(Collectors.toCollection(CopyOnWriteArrayList::new));
    }

    private Future<ClientChannel> randomAcquire() {
        SocketAddress socketAddress = bootstrapAddress.get(
                bootstrapAddress.size() == 1 ? 0 : ThreadLocalRandom.current().nextInt(bootstrapAddress.size()));
        List<Future<ClientChannel>> futures = channelPools.get(socketAddress);

        if (futures != null && !futures.isEmpty()) {
            List<Future<ClientChannel>> actives = futures.stream()
                    .filter(healthChecker::isHealthy).collect(Collectors.toCollection(CopyOnWriteArrayList::new));

            if (!actives.isEmpty()) {
                if (actives.size() == 1) {
                    return actives.get(0);
                }
                return actives.get(ThreadLocalRandom.current().nextInt(actives.size()));
            }
        }
        return newChannel(socketAddress);
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
            promise.tryFailure(
                    new IllegalStateException(String.format("Channel<%s> was closed", f.channel().toString())));
            assembleChannels.remove(f.channel().id().asLongText());
        });
        return promise;
    }

    private void flip(SocketAddress address, int limit) {
        if (address == null || limit <= 0) {
            throw new IllegalArgumentException("Address cannot be null, or limit expect > 0");
        }

        for (int i = 0; i < limit; i++) {
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
                    logger.error("Failed to new healthy chanel<{}>, address={}", client.getName(), address.toString());
                }
            }
        }
    }

    @Override
    public Promise<ClientChannel> assemblePromise(Channel channel) {
        return assembleChannels.computeIfAbsent(channel.id().asLongText(), v -> newImmediatePromise());
    }

    @Override
    public void shutdownGracefully() throws Exception {
        Set<Map.Entry<SocketAddress, List<Future<ClientChannel>>>> entries = channelPools.entrySet();
        for (Map.Entry<SocketAddress, List<Future<ClientChannel>>> entry : entries) {
            List<Future<ClientChannel>> futures = entry.getValue();
            if (futures.isEmpty()) {
                continue;
            }

            for (Future<ClientChannel> future : futures) {
                ClientChannel clientChannel = future.getNow();
                clientChannel.close();

                future.cancel(true);
            }
        }
    }
}
