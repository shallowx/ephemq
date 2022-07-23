package org.shallow;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollDatagramChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.resolver.dns.DefaultDnsServerAddressStreamProvider;
import io.netty.resolver.dns.DnsNameResolverBuilder;
import io.netty.resolver.dns.RoundRobinDnsAddressResolverGroup;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.pool.ChannelPoolFactory;

import static org.shallow.ObjectUtil.checkNotNull;
import static org.shallow.util.NetworkUtil.*;

public class Client {
    private static final InternalLogger logger = InternalLoggerFactory.getLogger(Client.class);

    private final String name;
    private final ClientConfig config;
    private Boolean state;

    private Bootstrap bootstrap;
    private EventLoopGroup workGroup;

    public Client(String name, ClientConfig config) {
        this.name = name;
        this.config = checkNotNull(config, "Client config cannot be null");
    }

    public void start() {
        if (state != null) {
            return;
        }
        state = Boolean.TRUE;
        workGroup = newEventLoopGroup(config.isEpollPrefer(), config.getWorkThreadWholes(), "client-worker(" + name + ")");

        DnsNameResolverBuilder drb = new DnsNameResolverBuilder();
        drb.ttl(config.getDnsTtlMaxExpiredSeconds(), config.getDnsTtlMaxExpiredSeconds());
        drb.negativeTtl(config.getNegativeTtlSeconds());

        drb.channelType((config.isEpollPrefer() && Epoll.isAvailable()) ? EpollDatagramChannel.class : NioDatagramChannel.class);
        drb.nameServerProvider(DefaultDnsServerAddressStreamProvider.INSTANCE);

        bootstrap = new Bootstrap()
                .group(workGroup)
                .channel(preferChannelClass(config.isEpollPrefer()))
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, false)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 30000)
                .option(ChannelOption.SO_SNDBUF, 65536)
                .option(ChannelOption.SO_RCVBUF, 65536)
                .resolver(new RoundRobinDnsAddressResolverGroup(drb));

        ChannelPoolFactory.INSTANCE.newChannelPool(bootstrap, config);
    }

    public void shutdownGracefully() {
        if (state != Boolean.TRUE) {
            return;
        }
        state = Boolean.FALSE;

        if (workGroup != null) {
            workGroup.shutdownGracefully();
        }
    }
}
