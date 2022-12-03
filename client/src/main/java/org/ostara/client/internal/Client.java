package org.ostara.client.internal;

import static org.ostara.remote.util.NetworkUtils.newEventLoopGroup;
import static org.ostara.remote.util.NetworkUtils.preferChannelClass;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollDatagramChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.resolver.dns.DefaultDnsServerAddressStreamProvider;
import io.netty.resolver.dns.DnsNameResolverBuilder;
import io.netty.resolver.dns.RoundRobinDnsAddressResolverGroup;
import org.ostara.client.internal.metadata.MetadataWriter;
import org.ostara.client.internal.pool.DefaultFixedChannelPoolFactory;
import org.ostara.client.internal.pool.ShallowChannelHealthChecker;
import org.ostara.client.internal.pool.ShallowChannelPool;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.common.util.ObjectUtils;

public class Client {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(Client.class);

    private final String name;
    private final ClientConfig config;
    private MetadataWriter manager;
    private volatile boolean state = false;
    private Bootstrap bootstrap;
    private EventLoopGroup workGroup;
    private final ShallowChannelHealthChecker healthChecker;
    private ClientListener listener;
    private ShallowChannelPool pool;

    public Client(String name, ClientConfig config) {
        this(name, config, null);
    }

    public Client(String name, ClientConfig config, ClientListener listener) {
        this(name, config, listener, null);
    }

    public Client(String name, ClientConfig config, ClientListener listener,
                  ShallowChannelHealthChecker healthChecker) {
        this.name = ObjectUtils.checkNonEmpty(name, "Client name cannot be empty");
        this.config = ObjectUtils.checkNotNull(config, "Client config cannot be null");
        this.listener = listener;
        this.healthChecker = healthChecker;
    }

    public void start() throws Exception {
        if (state) {
            return;
        }
        state = true;
        workGroup =
                newEventLoopGroup(config.isEpollPrefer(), config.getWorkThreadLimit(), "client-worker(" + name + ")");

        DnsNameResolverBuilder drb = new DnsNameResolverBuilder();
        drb.ttl(config.getDnsTtlMaxExpiredSeconds(), config.getDnsTtlMaxExpiredSeconds());
        drb.negativeTtl(config.getNegativeTtlSeconds());

        drb.channelType((config.isEpollPrefer() && Epoll.isAvailable()) ? EpollDatagramChannel.class :
                NioDatagramChannel.class);
        drb.nameServerProvider(DefaultDnsServerAddressStreamProvider.INSTANCE);

        bootstrap = new Bootstrap()
                .group(workGroup)
                .channel(preferChannelClass(config.isEpollPrefer()))
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, false)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getConnectTimeOutMs())
                .option(ChannelOption.SO_SNDBUF, 65536)
                .option(ChannelOption.SO_RCVBUF, 65536)
                .resolver(new RoundRobinDnsAddressResolverGroup(drb));

        pool = DefaultFixedChannelPoolFactory.INSTANCE.newChannelPool(this);

        this.manager = new MetadataWriter(this);
        manager.start();

        if (logger.isInfoEnabled()) {
            logger.info("The client<{}> started successfully", name);
        }
    }

    public String getName() {
        return name;
    }

    public ClientListener getListener() {
        return listener;
    }

    public void registerListener(ClientListener listener) {
        this.listener = listener;
    }

    public ShallowChannelPool getChanelPool() {
        return pool;
    }

    public ClientConfig getClientConfig() {
        return config;
    }

    public Bootstrap getBootstrap() {
        return bootstrap;
    }

    public ShallowChannelHealthChecker getHealthChecker() {
        return healthChecker;
    }

    public MetadataWriter getMetadataWriter() {
        return manager;
    }

    public synchronized void shutdownGracefully() throws Exception {
        if (state != Boolean.TRUE) {
            return;
        }
        state = Boolean.FALSE;

        pool.shutdownGracefully();

        manager.shutdownGracefully(() -> {
            if (workGroup != null) {
                workGroup.shutdownGracefully();
            }
            return null;
        });
    }
}
