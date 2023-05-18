package org.ostara.client.internal;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollDatagramChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.resolver.dns.DefaultDnsServerAddressStreamProvider;
import io.netty.resolver.dns.DnsNameResolverBuilder;
import io.netty.resolver.dns.RoundRobinDnsAddressResolverGroup;
import io.netty.util.concurrent.*;
import io.netty.util.concurrent.Future;
import org.ostara.client.ClientConfig;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.remote.codec.MessageDecoder;
import org.ostara.remote.codec.MessageEncoder;
import org.ostara.remote.handle.ConnectDuplexHandler;
import org.ostara.remote.handle.ProcessDuplexHandler;
import org.ostara.remote.invoke.InvokeAnswer;
import org.ostara.remote.processor.ProcessorAware;
import org.ostara.remote.util.NetworkUtils;

import javax.annotation.Nonnull;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class Client {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(Client.class);

    private String name;
    private ClientConfig config;
    private ClientListener listener;
    private List<SocketAddress> bootstrapAddress;
    private EventLoopGroup workerGroup;
    private Bootstrap bootstrap;
    private EventExecutor taskExecutor;
    private volatile Boolean state;

    public Client(String name, ClientConfig config, ClientListener listener) {
        this.name = name;
        this.config = Objects.requireNonNull(config, "client config not found");
        this.listener = Objects.requireNonNull(listener, "client lister not found");
        this.bootstrapAddress = NetworkUtils.switchSocketAddress(config.getBootstrapAddresses());
    }

    private SocketAddress bootstrapAddress() {
        int size = bootstrapAddress.size();
        if (size == 0) {
            return null;
        }

        if (size == 1) {
           return bootstrapAddress.get(0);
        }

        return bootstrapAddress.get(ThreadLocalRandom.current().nextInt(size));
    }

    private final Map<SocketAddress, List<Future<ClientChannel>>> channels = new ConcurrentHashMap<>();
    public ClientChannel fetchChannel(SocketAddress address) {
        try {
            return acquireChannel(address).get(config.getChannelConnectionTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (Throwable t) {
            if (address == null) {
                throw new RuntimeException("Fetch random client channel failed", t);
            }
            throw new RuntimeException(String.format("Fetch random client channel failed, address=%s", address), t);
        }
    }

    @Nonnull
    private Future<ClientChannel> acquireChannel(SocketAddress address) {
        Future<ClientChannel> future;
        if (address == null) {
            future = randomAcquire();
            if (future != null) {
                return future;
            }

            address = bootstrapAddress();
            if (address == null) {
                throw new IllegalArgumentException("Bootstrap address not found");
            }
        }

        List<Future<ClientChannel>> futures = listValidChannels(address);
        if (futures != null && futures.size() >= config.getConnectionPoolCapacity()) {
            return futures.get(ThreadLocalRandom.current().nextInt(futures.size()));
        }

        synchronized (channels) {
            futures = listValidChannels(address);
            if (futures != null && futures.size() >= config.getConnectionPoolCapacity()) {
                return futures.get(ThreadLocalRandom.current().nextInt(futures.size()));
            }

            future = createChannelFuture(address);
            channels.computeIfAbsent(address, k -> new CopyOnWriteArrayList<>()).add(future);
            final SocketAddress theAddress = address;
            future.addListener((GenericFutureListener<Future<ClientChannel>>) f -> {
               if (!f.isSuccess()) {
                   removeChannel(theAddress, f);
               } else {
                   f.getNow().onClosed(() -> removeChannel(theAddress, f));
               }
            });

            return future;
        }
    }

    private void removeChannel(SocketAddress address, Future<ClientChannel> future) {
        synchronized (channels) {
            List<Future<ClientChannel>> futures = channels.get(address);
            if (futures == null) {
                return;
            }

            futures.remove(future);
            if (futures.isEmpty()) {
                channels.remove(address);
            }
        }
    }

    private final ConcurrentMap<String, Promise<ClientChannel>> assembleChannels = new ConcurrentHashMap<>();

    public Future<ClientChannel> createChannelFuture(SocketAddress address) {
        Bootstrap theBootstrap = bootstrap.clone().handler(new InnerChannelInitializer(address));
        ChannelFuture channelFuture = theBootstrap.connect(address);
        Channel channel = channelFuture.channel();
        Promise<ClientChannel> assemblePromise = assembleChannels
                .computeIfAbsent(channel.id().asLongText(), k -> ImmediateEventExecutor.INSTANCE.newPromise());
        channelFuture.addListener(future -> {
           if (!future.isSuccess()) {
               assemblePromise.tryFailure(future.cause());
               assembleChannels.remove(channel.id().asLongText());
           }
        });

        channel.closeFuture().addListener(f -> {
           assemblePromise.tryFailure(new IllegalArgumentException("Client channel is closed"));
            assembleChannels.remove(channel.id().asLongText());
        });

        return assemblePromise;
    }

    private List<Future<ClientChannel>> listValidChannels(SocketAddress address) {
        List<Future<ClientChannel>> futures = channels.get(address);
        return futures == null ? null : futures.stream().filter(this::isValidChannel).collect(Collectors.toList());
    }

    private boolean isValidChannel(Future<ClientChannel> future) {
        if (future == null) {
            return false;
        }

        if (!future.isDone()) {
            return true;
        }

        if (!future.isSuccess()) {
            return false;
        }

        ClientChannel channel = future.getNow();
        return channel != null && channel.isActive();
    }

    private Future<ClientChannel> randomAcquire() {
        int size = channels.size();
        if (size == 0) {
            return null;
        }
        List<Future<ClientChannel>> validChannels = channels.values().stream()
                .flatMap(Collection::stream)
                .filter(this::isValidChannel).toList();

        if (validChannels.isEmpty()) {
            return null;
        }

        if (validChannels.size() == 1) {
            return validChannels.get(0);
        }

        return validChannels.get(ThreadLocalRandom.current().nextInt(validChannels.size()));
    }

    public boolean isRunning() {
        return state != null && state;
    }

    public synchronized void start() {
        if (state != null) {
            return;
        }

        state = Boolean.TRUE;
        workerGroup = NetworkUtils.newEventLoopGroup(config.isSocketEpollPrefer(), config.getWorkerThreadCount(), "client-worker(" + name + ")");
        DnsNameResolverBuilder builder = new DnsNameResolverBuilder();
        builder.ttl(30, 300);
        builder.negativeTtl(30);

        if (config.isSocketEpollPrefer() && Epoll.isAvailable()) {
            builder.channelType(EpollDatagramChannel.class);
        } else {
            builder.channelType(NioDatagramChannel.class);
        }

        builder.nameServerProvider(DefaultDnsServerAddressStreamProvider.INSTANCE);
        bootstrap = new Bootstrap()
                .group(workerGroup)
                .channel(NetworkUtils.preferChannelClass(config.isSocketEpollPrefer()))
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, false)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getChannelConnectionTimeoutMs())
                .option(ChannelOption.SO_SNDBUF, config.getSocketSendBufferSize())
                .option(ChannelOption.SO_RCVBUF, config.getSocketReceiveBufferSize())
                .resolver(new RoundRobinDnsAddressResolverGroup(builder));
        for (SocketAddress address : bootstrapAddress) {
            acquireChannel(address);
        }

        taskExecutor = new DefaultEventExecutor(new DefaultThreadFactory("client("+ name +")-task"));
        taskExecutor.schedule(new RefreshMetadataTask(), config.getMetadataRefreshPeriodMs(), TimeUnit.MILLISECONDS);
    }

    private synchronized void close() {
        if (state != Boolean.TRUE) {
            return;
        }

        state = Boolean.FALSE;
        if (taskExecutor != null) {
            Future<?> future = taskExecutor.shutdownGracefully();
            future.addListener(f -> {
                if (workerGroup != null) {
                    workerGroup.shutdownGracefully();
                }
            });
        }
    }

    private static class RefreshMetadataTask implements Runnable {
        @Override
        public void run() {

        }
    }

    private class InnerChannelInitializer extends ChannelInitializer<SocketChannel> {

        private final SocketAddress address;

        public InnerChannelInitializer(SocketAddress address) {
            this.address = address;
        }

        @Override
        protected void initChannel(SocketChannel socketChannel) throws Exception {
            ClientChannel clientChannel = new ClientChannel(config, socketChannel, address);
            socketChannel.pipeline()
                    .addLast("packet-encoder", MessageEncoder.instance())
                    .addLast("packet-decoder", new MessageDecoder())
                    .addLast("connect-handler", new ConnectDuplexHandler(
                            config.getChannelKeepPeriodMs(), config.getChannelIdleTimeoutMs()
                            ))
                    .addLast("service-handler", new ProcessDuplexHandler(new InnerServiceProcessor(clientChannel)));
        }

        private class InnerServiceProcessor implements ProcessorAware {

            private final ClientChannel clientChannel;

            public InnerServiceProcessor(ClientChannel channel) {
                this.clientChannel = channel;
            }

            @Override
            public void onActive(Channel channel, EventExecutor executor) {
               try {
                  assembleChannels.computeIfAbsent(channel.id().asLongText(), k -> ImmediateEventExecutor.INSTANCE.newPromise()).setSuccess(clientChannel);
                  channel.closeFuture().addListener((ChannelFutureListener) f -> listener.onChannelClosed(clientChannel));
                  listener.onChannelActive(clientChannel);
               }catch (Throwable t){
                   channel.close();
               }
            }

            @Override
            public void process(Channel channel, byte command, ByteBuf data, InvokeAnswer<ByteBuf> answer) {

            }
        }
    }
}
